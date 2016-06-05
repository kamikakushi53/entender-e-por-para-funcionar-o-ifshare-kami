from __future__ import print_function

__author__ = 'dclobato'

import random
import sys
import tempfile
import bitstring

from boto.s3.key import Key
from config_file import *
from utilities import getObjectStoreConnection, createAndGetObjectStoreBucket, connectDB

def teardown(exitCode = 0):
    print("")
    print("-" * 80)
    print("Encerrando as conexoes")
    print("-" * 80)
    print("Encerrando a conexao com o banco")
    db.close()
    print("Encerrando conexao com os buckets do provedores de armazenamento")
    for i in range(CONFIG['storage']['numsites']):
        print("   %s" % (CONFIG['storage'][i]['PROVIDER']))
        s3Upload[i].close()
    print("=-" * 40)
    print("Fim dos trabalhos!")
    print("=-" * 40)
    sys.exit(exitCode)

random.seed()

db = connectDB(CONFIG['DB'])

s3Upload = []
upBucket = []

print("Estabelecendo conexao com os provedores de armazenamento...")
for i in range(CONFIG['storage']['numsites']):
    print("   %s... " % (CONFIG['storage'][i]['PROVIDER']))

    tmpS3Connection = getObjectStoreConnection(CONFIG['storage'][i], debug=False)
    tmpUpBucket = createAndGetObjectStoreBucket(CONFIG['storage'][i], tmpS3Connection, debug=False)

    s3Upload.append(tmpS3Connection)
    upBucket.append(tmpUpBucket)

print("")
print("-" * 80)
print("Reparando os arquivos")
print("-" * 80)

cursor = db.cursor()
qstmt = "SELECT COUNT(sha1parte) from partes;"
cursor.execute(qstmt)
rs = cursor.fetchall()
cursor.close()
numPartes = rs[0][0]

if (numPartes == 0):
    teardown("Nada para fazer...")

numEsperadoReplicas = CONFIG['storage']['numcopies']

cursor = db.cursor()
qstmt = "SELECT sha1arquivo, nome, tamanho, datahora, chunksize, numchunks FROM arquivos;"
cursor.execute(qstmt)
rsArquivos = cursor.fetchall()
cursor.close()

for arquivo in rsArquivos:
    distReplicas = [0] * CONFIG['storage']['numsites']
    print("Reparando o arquivo %s..." % (arquivo[1]))

    for chunk in range(arquivo[5]):
        bitMapExistente = bitstring.BitArray(CONFIG['storage']['numsites'])
        bitMapEsperado  = bitstring.BitArray(CONFIG['storage']['numsites'])

        cursor = db.cursor()
        qstmt = "SELECT sha1arquivo, sha1parte, chunk, onde FROM partes WHERE sha1arquivo = %s AND chunk = %s;"
        qdata = (arquivo[0], chunk)
        cursor.execute(qstmt, qdata)
        rsReplicas = cursor.fetchall()
        cursor.close()

        sha1parte = rsReplicas[0][1]

        print("   Processando chunk %d" % (chunk))

        for (sha1arquivo, sha1parte, chunkreplica, onde) in rsReplicas:
            print(onde)
            print(bitMapExistente)
            print(bitMapEsperado)
            bitMapEsperado.set(1, onde)
            filename = str(sha1arquivo) + "." + str(sha1parte) + ".ifsharechunk"
            print("      %-20s (%02d)... " % (CONFIG['storage'][onde]['PROVIDER'], onde), end="")
            if upBucket[int(onde)].get_key(filename) is None:
                print("AUSENTE")
                bitMapExistente.set(0, onde)
                c = db.cursor()
                qstmt = "DELETE FROM partes WHERE sha1arquivo = %s AND chunk = %s AND onde = %s;"
                qdata = (sha1arquivo, str(chunkreplica), str(onde))
                c.execute(qstmt, qdata)
                db.commit()
                c.close()
            else:
                print("Presente")
                # Verificar se o conteudo esta correto tambem, pelo sha1
                # Se nao estiver, eh a mesma coisa que uma replica ausente, e tratar de acordo
                bitMapExistente.set(1, int(onde))
                distReplicas[onde] += 1

        numExistenteReplicas = bitMapExistente.count(1)

        if (numExistenteReplicas == numEsperadoReplicas):
            print("      !! Todas as replicas do chunk %d estao presentes" % (chunk))
        else:
            if (numExistenteReplicas == 0):
                print("      !! Todas as replicas estao faltando. Impossivel reparar o arquivo!")
                # fazer a limpeza do banco e dos buckets, removendo o que sobrou
            else:
                replicasFaltantes = numEsperadoReplicas - numExistenteReplicas
                print("      Falta(m) %d replica(s)" % (replicasFaltantes))
                print("      Distribuindo as replicas faltantes")
                for i in range (0, replicasFaltantes):
                    idBucketOrigem  = random.choice(list(bitMapExistente.findall(bitstring.Bits(bin="0b1"))))
                    idBucketDestino = random.choice(list(bitMapExistente.findall(bitstring.Bits(bin="0b0"))))
                    print("         %-20s (%02d) --> %-20s (%02d)... " % \
                          (CONFIG['storage'][idBucketOrigem]['PROVIDER'], idBucketOrigem, \
                           CONFIG['storage'][idBucketDestino]['PROVIDER'], idBucketDestino), end="")
                    print("Get... ", end="")
                    chaveOrigem = Key(upBucket[idBucketOrigem])
                    chaveOrigem.key = filename
                    fd = tempfile.NamedTemporaryFile(delete=True)
                    chaveOrigem.get_contents_to_file(fd)
                    fd.seek(0, 0)
                    # ANTES DE SUBIR
                    # verificar SHA1 do que baixou. Se nao estiver ok, baixar novamente
                    print("Set... ", end="")
                    chaveDestino = Key(upBucket[idBucketDestino])
                    chaveDestino.key = filename
                    chaveDestino.set_contents_from_file(fd)
                    distReplicas[idBucketDestino] += 1
                    bitMapExistente.set(1, idBucketDestino)

                    c = db.cursor()
                    qstmt = "INSERT INTO partes (sha1arquivo, sha1parte, chunk, onde) VALUES (%s, %s, %s, %s)"
                    qdata = (arquivo[0], sha1parte, chunk, idBucketDestino)
                    if (CONFIG['DOBDINSERT']):
                        c.execute(qstmt, qdata)
                        db.commit()
                    print("Done!")
                    c.close()

    print("")
    print("   Distribuicao das replicas pelos repositorios")
    print("   %20s %6s" % ("-" * 20, "-" * 6))
    print("   %-20s %6s" % ("Provedor", "Chunks"))
    print("   %20s %6s" % ("-" * 20, "-" * 6))
    for i in range(CONFIG['storage']['numsites']):
        print("   %-20s %6d" % (CONFIG['storage'][i]['PROVIDER'], distReplicas[i]))
    print("   %20s %6s" % ("-" * 20, "-" * 6))
    print("")
    print("")

teardown()