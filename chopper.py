from __future__ import print_function
import hashlib

__author__ = 'dclobato'

import os
import random
import sys
import time
import tempfile

import mysql.connector
import boto
import boto.sqs
import boto.sqs.message
from boto.s3.connection import OrdinaryCallingFormat
from filechunkio import FileChunkIO
from progressbar import ProgressBar, Percentage, Bar, ETA, FileTransferSpeed
from boto.s3.key import Key

from config_file import *
from utilities import bestSize, readMetadata, getObjectStoreConnection, createAndGetObjectStoreBucket

random.seed()

# print(json.dumps(CONFIG, sort_keys=True, indent=2))

if (CONFIG['storage']['numcopies'] > CONFIG['storage']['numsites']):
    print("Numero de copias maior que o numero de sites de armazenamento!")
    sys.exit("Erro de configuracao")

print("-" * 80)
print("Informacoes sobre o modo de operacao")
print("-" * 80)
print("Operando %s insercoes no banco de dados" % ("SEM", "COM")[(CONFIG['DOBDINSERT'] == True)])
print("Operando %s remocao de fila e dos arquivos do bucket de entrada" % ("SEM", "COM")[(CONFIG['DOREMOVE'] == True)])
print("Operando %s remocao do arquivo baixado do S3 de entrada" % ("SEM", "COM")[(CONFIG['DOREMOVELOCAL'] == True)])
print("Operando %s com upload das partes para os repositorios" % ("SEM", "COM")[CONFIG['DOBUCKETUPLOAD'] == True])

print("")
print("-" * 80)
print("Estabelecendo conexoes iniciais")
print("-" * 80)
print("Conectando aa fila de arquivos a processar...")
sqsConnection = boto.sqs.connect_to_region(CONFIG['Incoming']['REGION'],
                                           aws_access_key_id=CONFIG['Incoming']['ACCESS_KEY'],
                                           aws_secret_access_key=CONFIG['Incoming']['SECRET_KEY'])
myQueue = sqsConnection.lookup(CONFIG['Incoming']['QUEUE'])
if myQueue is None:
    sys.exit("Nao existe a fila de arquivos a processar!")

print("Conectando ao bucket de arquivos a processar...")
s3Incoming = boto.s3.connect_to_region(CONFIG['Incoming']['REGION'],
                                       aws_access_key_id=CONFIG['Incoming']['ACCESS_KEY'],
                                       aws_secret_access_key=CONFIG['Incoming']['SECRET_KEY'],
                                       calling_format=OrdinaryCallingFormat())
bucket = s3Incoming.lookup(CONFIG['Incoming']['BUCKET'])
if bucket is None:
    sqsConnection.close()
    sys.exit("Nao existe o bucket de arquivos temporarios!")

print("Conectando ao banco de dados...")
db = mysql.connector.connect(user=CONFIG['DB']['USER'],
                             password=CONFIG['DB']['PASS'],
                             host=CONFIG['DB']['ENDPOINT'],
                             database=CONFIG['DB']['DATABASE'])

if (CONFIG['DOBUCKETUPLOAD']):
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
print("Processando os arquivos")
print("-" * 80)

numArquivos = myQueue.count()
print("Ha um total de %d arquivos na fila para serem processados" % (numArquivos))

for arquivo in range(0, numArquivos):
    mensagem = myQueue.read(10)
    if mensagem is None:
        db.close()
        s3Incoming.close()
        if (CONFIG['DOBUCKETUPLOAD']):
            for i in range(CONFIG['storage']['numsites']):
                s3Upload[i].close()
        sqsConnection.close()
        sys.exit("Sumiu mensagem da fila. Talvez mais de um chopper em execucao?")
    nomeArquivoMetadados = str(mensagem.get_body())
    print("Processando o arquivo %2d de %2d >> [%s]" % (arquivo + 1, numArquivos, nomeArquivoMetadados))

    chave = bucket.get_key(nomeArquivoMetadados)
    if chave is None:
        db.close()
        s3Incoming.close()
        if (CONFIG['DOBUCKETUPLOAD']):
            for i in range(CONFIG['storage']['numsites']):
                s3Upload[i].close()
        sqsConnection.close()
        sys.exit("Arquivo de metadados sumiu do S3!")

    fd = tempfile.NamedTemporaryFile(delete=True)
    chave.get_contents_to_file(fd)
    fd.seek(0, 0)

    ifshare_propriedades = dict()

    readMetadata(fd, ifshare_propriedades)

    fd.close()

    cursor = db.cursor()

    def _dl_progress_cb(trans, total):
        pbar.update(trans)

    widgets = ["Baixando o arquivo ", Percentage(), " ", Bar(marker='#', left='[', right=']'), " ", ETA(), " ", FileTransferSpeed()]

    chaveEntrada = bucket.get_key(ifshare_propriedades['arquivo'])
    nomeArquivoEntrada = ifshare_propriedades['hashgeral'] + ".incoming"

    pbar = ProgressBar(widgets=widgets, maxval=chaveEntrada.size, term_width=80)
    pbar.start()
    chaveEntrada.get_contents_to_filename(nomeArquivoEntrada, cb=_dl_progress_cb, num_cb=50)
    pbar.finish()

    fd = open(nomeArquivoEntrada, "rb")

    widgets = ["Verificando arquivo ", Percentage(), " ", Bar(marker='#', left='[', right=']'), " ", ETA()]

    pbar = ProgressBar(widgets=widgets, maxval=ifshare_propriedades['numchunks'], term_width=80)
    pbar.start()

    hashergeral = hashlib.sha1()
    for i in range(ifshare_propriedades['numchunks']):
        fd.seek(i * ifshare_propriedades['chunksize'])
        hasher = hashlib.sha1()
        buf = fd.read(ifshare_propriedades['chunksize'])
        hasher.update(buf)
        hashergeral.update(buf)
        if (ifshare_propriedades['hashchunk' + str(i)] != hasher.hexdigest()):
            print(">> Chunk %d esta incorreto na copia local" % i)

        pbar.update(i)

    if (ifshare_propriedades['hashgeral'] != hashergeral.hexdigest()):
        print(">> Hash geral da copia local (%s) diferente do esperado (%s)" % (hashergeral.hexdigest(), ifshare_propriedades['hashgeral']))
        if (CONFIG['DOREMOVE']):
            myQueue.delete_message(mensagem)
            bucket.delete_key(nomeArquivoMetadados)
            bucket.delete_key(ifshare_propriedades['arquivo'])
        continue

    pbar.finish()

    fd.close()

    print("")
    print("Subindo os chunks para os provedores...")

    totalBytes = [0] * CONFIG['storage']['numsites']
    totalChunks = [0] * CONFIG['storage']['numsites']
    bytesAcumulados = 0.00
    chunksAcumulados = 0.00

    try:
        add_arquivo = ("INSERT INTO arquivos "
                       "(sha1arquivo, nome, tamanho, datahora, chunksize, numchunks) "
                       "VALUES (%(hashgeral)s, %(arquivo)s, %(tamanho)s, %(criacao_do_arquivo)s, "
                       "%(chunksize)s, %(numchunks)s)")

        if (CONFIG['DOBDINSERT']):
            cursor.execute(add_arquivo, ifshare_propriedades)
            db.commit()

    except mysql.connector.errors.IntegrityError as err:
        print(">> Arquivo ja havia sido processado (hash %s)" % ifshare_propriedades['hashgeral'])
        if (CONFIG['DOREMOVE']):
            myQueue.delete_message(mensagem)
            bucket.delete_key(nomeArquivoMetadados)
            bucket.delete_key(ifshare_propriedades['arquivo'])
        continue

    add_parte = "INSERT INTO partes (sha1arquivo, sha1parte, chunk, onde) VALUES (%s, %s, %s, %s)"

    for chunk in range(ifshare_propriedades['numchunks']):
        storageSites = random.sample(range(CONFIG['storage']['numsites']), CONFIG['storage']['numcopies'])
        storageSites.sort()
        print("   Chunk %4d de %4d vai para >> " % (chunk + 1, ifshare_propriedades['numchunks']), end="")

        for i in range(CONFIG['storage']['numcopies']):
            print("[%-20s] " % (CONFIG['storage'][storageSites[i]]['PROVIDER']), end="")
            if (chunk == ifshare_propriedades['numchunks'] - 1):
                qtosBytes = ifshare_propriedades['tamanho'] - chunk * ifshare_propriedades['chunksize']
            else:
                qtosBytes = ifshare_propriedades['chunksize']
            totalBytes[storageSites[i]] += qtosBytes
            totalChunks[storageSites[i]] += 1
            bytesAcumulados += qtosBytes
            chunksAcumulados += 1

            if (CONFIG['DOBUCKETUPLOAD']):
                offset = ifshare_propriedades['chunksize'] * chunk
                fpParte = FileChunkIO(nomeArquivoEntrada, offset=offset, bytes=qtosBytes)
                chaveNoBucket = Key(upBucket[storageSites[i]])
                chaveNoBucket.key = ifshare_propriedades['hashgeral'] + "." + ifshare_propriedades[
                    'hashchunk' + str(chunk)] + ".ifsharechunk"
                chaveNoBucket.set_contents_from_file(fpParte)

            data_parte = (ifshare_propriedades['hashgeral'], ifshare_propriedades['hashchunk' + str(chunk)], chunk, storageSites[i])
            if (CONFIG['DOBDINSERT']):
                cursor.execute(add_parte, data_parte)
        print("")

        if (CONFIG['DOBDINSERT']):
            db.commit()
    cursor.close()
    if (CONFIG['DOREMOVELOCAL']):
        try:
            os.remove(nomeArquivoEntrada)
        except OSError:
            pass
    print("")
    print("   %20s %15s %6s" % ("-" * 20, "-" * 15, "-" * 6))
    print("   %-20s %15s %6s" % ("Provedor", "Bytes", "Chunks"))
    print("   %20s %15s %6s" % ("-" * 20, "-" * 15, "-" * 6))
    for i in range(CONFIG['storage']['numsites']):
        print("   %-20s %15s %6d" % (CONFIG['storage'][i]['PROVIDER'], bestSize(totalBytes[i]), totalChunks[i]))
    print("   %20s %15s %6s" % ("-" * 20, "-" * 15, "-" * 6))
    print("   %20s %15s %6d" % ("Total geral", bestSize(bytesAcumulados), chunksAcumulados))
    print("   %20s %15s %6d" % ("Tamanho do arquivo", bestSize(ifshare_propriedades['tamanho']), ifshare_propriedades['numchunks']))
    overheadBytes = (bytesAcumulados / ifshare_propriedades['tamanho'] - 1) * 100
    overheadChunks = (chunksAcumulados / ifshare_propriedades['numchunks'] - 1) * 100
    print("   %20s %15.4f%% %5.1f%%" % ("Overhead", overheadBytes, overheadChunks))

    print("")

    if (CONFIG['DOREMOVE']):
        myQueue.delete_message(mensagem)
        print("   Removendo arquivo de metadados do repositorio de entrada")
        bucket.delete_key(nomeArquivoMetadados)
        print("   Removendo arquivo do repositorio de entrada")
        bucket.delete_key(ifshare_propriedades['arquivo'])
        print("")

print("")
print("-" * 80)
print("Encerrando as conexoes")
print("-" * 80)
print("Encerrando a conexao com o banco")
db.close()
print("Encerrando a conexao com o bucket de arquivos a processar")
s3Incoming.close()
if (CONFIG['DOBUCKETUPLOAD']):
    print("Encerrando conexao com os buckets do provedores de armazenamento")
    for i in range(CONFIG['storage']['numsites']):
        print("   %s" % (CONFIG['storage'][i]['PROVIDER']))
        s3Upload[i].close()
print("Encerrando a conexao com a fila de arquivos a processar")
sqsConnection.close()
print("=-" * 40)
print("Fim dos trabalhos!")
print("=-" * 40)
sys.exit()
