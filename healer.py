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
print("Reparando os arquivos")
print("-" * 80)

cursor = db.cursor()
query = "SELECT COUNT(sha1parte) from partes;"
cursor.execute(query)
rs = cursor.fetchall()
cursor.close()
numPartes = rs[0][0]

cursor = db.cursor()
query = "SELECT COUNT(sha1arquivo) from arquivos;"
cursor.execute(query)
rs = cursor.fetchall()
cursor.close()
numArquivos = rs[0][0]

print("Ha um total de %d partes em %d arquivos para serem processadas" % (numPartes, numArquivos))

if (numPartes == 0):
    print("Nada para fazer...")
    print("")
    print("-" * 80)
    print("Encerrando as conexoes")
    print("-" * 80)
    print("Encerrando a conexao com o banco")
    db.close()
    if (CONFIG['DOBUCKETUPLOAD']):
        print("Encerrando conexao com os buckets do provedores de armazenamento")
        for i in range(CONFIG['storage']['numsites']):
            print("   %s" % (CONFIG['storage'][i]['PROVIDER']))
            s3Upload[i].close()
    print("=-" * 40)
    print("Fim dos trabalhos!")
    print("=-" * 40)
    sys.exit()

listaProvedores = []
for i in range(CONFIG['storage']['numsites']):
    listaProvedores.append(CONFIG['storage'][i]['PROVIDER'])

cursor = db.cursor()
query = "SELECT sha1arquivo, nome, tamanho, datahora, chunksize, numchunks FROM arquivos;"
cursor.execute(query)
rsArquivos = cursor.fetchall()
cursor.close()

for arquivo in range(0, numArquivos):
    print("Reparando o arquivo %s..." % (rsArquivos[arquivo][1]))

    for chunk in range(rsArquivos[arquivo][5]):
        cursor = db.cursor()
        query = "SELECT sha1arquivo, sha1parte, chunk, onde FROM partes WHERE sha1arquivo = '" + rsArquivos[arquivo][0] + "' AND chunk = " + str(
            chunk)
        cursor.execute(query)
        rsPartes = cursor.fetchall()
        for row in rsPartes:
            filename = str(row[0]) + "." + str(row[1]) + ".ifsharechunk"
            if upBucket[int(row[3])].get_key(filename) is None:
                print("Parte %d do arquivo %s esta faltando..." % (chunk, rsArquivos[arquivo][1]))
                #
                # Remover essa tupla do tabela de partes
                # Tentar baixar uma das outras replicas
                # Subir essa replica para outro repositorio (ou para o mesmo, up to you)
                # Atualizar a tabela de partes com essa informacao

            else:
                print("Parte %d do arquivo %s esta presente. Avante!" % (chunk, rsArquivos[arquivo][1]))


        cursor.close()

print("")
print("-" * 80)
print("Encerrando as conexoes")
print("-" * 80)
print("Encerrando a conexao com o banco")
db.close()
if (CONFIG['DOBUCKETUPLOAD']):
    print("Encerrando conexao com os buckets do provedores de armazenamento")
    for i in range(CONFIG['storage']['numsites']):
        print("   %s" % (CONFIG['storage'][i]['PROVIDER']))
        s3Upload[i].close()
print("=-" * 40)
print("Fim dos trabalhos!")
print("=-" * 40)
sys.exit()
