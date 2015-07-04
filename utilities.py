import sys

__author__ = 'dclobato'
import datetime

import boto
import time
import boto.sqs
import boto.sqs.message
from boto.s3.connection import OrdinaryCallingFormat
from boto.exception import S3ResponseError, S3CreateError
from boto.s3.key import Key


def bestSize(tamanho):
    if tamanho < 1024:
        return (str(tamanho) + "B  ")
    if tamanho < 1024 * 1024:
        return (str(round(tamanho / 1024.0, 2)) + "KiB")
    if tamanho < 1024 * 1024 * 1024:
        return (str(round(tamanho / 1024.0 / 1024.0, 2)) + "MiB")
    return (str(round(tamanho / 1024.0 / 1024.0 / 1024.0, 2)) + "GiB")


def showMetadata(prop_dict):
    print("")
    print("+%s" % ("-" * 62))
    print("| %s" % (prop_dict['arquivo']))
    print("+%s" % ("-" * 62))
    print("| Tamanho............: %s" % (bestSize(prop_dict['tamanho'])))
    print("| Tamanho do chunk...: %s (%d chunks no total)" % (bestSize(prop_dict['chunksize']), prop_dict['numchunks']))
    print("| Data de criacao....: %s" % (prop_dict['criacao_do_arquivo'].strftime("%Y/%m/%d %H:%M:%S")))
    print("| Hash geral.........: %s" % (prop_dict['hashgeral']))
    for chunk in range(0, prop_dict['numchunks']):
        print("|     Hash %05d.....: %s" % (chunk, prop_dict['hashchunk' + str(chunk)]))
    print("+%s" % ("-" * 62))
    return


def dumpMetadata(prop_dict):
    print(prop_dict['arquivo'])
    print(prop_dict['tamanho'])
    print(prop_dict['criacao_do_arquivo'].strftime("%Y%m%d%H%M%S"))
    print(prop_dict['hashgeral'])
    print(str(prop_dict['chunksize']))
    print(str(prop_dict['numchunks']))
    for chunk in range(prop_dict['numchunks']):
        print(prop_dict['hashchunk' + str(chunk)])
    return


def readMetadata(fd, prop_dict):
    prop_dict['arquivo'] = fd.readline().rstrip('\n')
    prop_dict['tamanho'] = int(fd.readline())
    prop_dict['criacao_do_arquivo'] = datetime.datetime.strptime(fd.readline().rstrip('\n'), "%Y%m%d%H%M%S")
    prop_dict['hashgeral'] = fd.readline().rstrip('\n')
    prop_dict['chunksize'] = int(fd.readline())
    prop_dict['numchunks'] = int(fd.readline())
    for chunk in range(0, prop_dict['numchunks']):
        prop_dict['hashchunk' + str(chunk)] = fd.readline().rstrip('\n')

    return


def writeMetadata(fd, prop_dict):
    fd.write(prop_dict['arquivo'] + "\n")
    fd.write(str(prop_dict['tamanho']) + "\n")
    fd.write(prop_dict['criacao_do_arquivo'].strftime("%Y%m%d%H%M%S") + "\n")
    fd.write(prop_dict['hashgeral'] + "\n")
    fd.write(str(prop_dict['chunksize']) + "\n")
    fd.write(str(prop_dict['numchunks']) + "\n")
    for chunk in range(prop_dict['numchunks']):
        fd.write(prop_dict['hashchunk' + str(chunk)] + "\n")

    return


def getObjectStoreConnection(objectStoreDict, debug=False):
    if (debug):
        print(
            "[DEBUG: getObjectStoreConnection]: Conectando ao repositorio %s (modo %s)" % (
                objectStoreDict['PROVIDER'], objectStoreDict['CT']))
    if (objectStoreDict['CT'] == "region"):
        osc = boto.s3.connect_to_region(objectStoreDict['REGION'],
                                        aws_access_key_id=objectStoreDict['ACCESS_KEY'],
                                        aws_secret_access_key=objectStoreDict['SECRET_KEY'],
                                        calling_format=OrdinaryCallingFormat())
    elif (objectStoreDict['CT'] == "host"):
        osc = boto.connect_s3(aws_access_key_id=objectStoreDict['ACCESS_KEY'],
                              aws_secret_access_key=objectStoreDict['SECRET_KEY'],
                              host=objectStoreDict['HOST'],
                              calling_format=OrdinaryCallingFormat())
    else:
        osc = boto.s3.connect_to_region(objectStoreDict['REGION'],
                                        aws_access_key_id=objectStoreDict['ACCESS_KEY'],
                                        aws_secret_access_key=objectStoreDict['SECRET_KEY'],
                                        host=objectStoreDict['HOST'],
                                        calling_format=OrdinaryCallingFormat())
    return (osc)


def createAndGetObjectStoreBucket(objectStoreDict, objectStoreConnection, debug=False):
    if (debug):
        print(
            "[DEBUG: createAndGetObjectStoreBucket]: Obtendo o bucket %s no repositorio %s" % (
                objectStoreDict['BUCKET'], objectStoreDict['PROVIDER']))
    try:
        osb = objectStoreConnection.head_bucket(objectStoreDict['BUCKET'])
    except S3ResponseError as err:
        if (err.status == 404):
            if (debug):
                print("[DEBUG: createAndGetObjectStoreBucket]: O bucket nao existe. Criando...")
            try:
                if (objectStoreDict['CT'] == "region"):
                    osb = objectStoreConnection.create_bucket(objectStoreDict['BUCKET'],
                                                              location=objectStoreDict['REGION'])
                else:
                    osb = objectStoreConnection.create_bucket(objectStoreDict['BUCKET'])
            except S3CreateError as err2:
                print(
                    "[DEBUG: createAndGetObjectStoreBucket]: O bucket nao existe e houve erro na criacao dele. Vamos tentar pegar na marra")
            finally:
                time.sleep(10)
            k = Key(osb)
            k.key = '000-DoNotTouchMe-000'
            k.set_contents_from_string('Este bucket e usado pelo projeto IFShare que implementa um sistema de arquivos distribuidos. Tanto o bucket quanto os arquivos que estao aqui dentro sao criticos para o funcionamento do sistema. Sendo assim, caia fora e va fazer outra coisa...')
            k = Key(osb)
            k.key = '001-DoNotTouchMe-001'
            k.set_contents_from_string('sim')
        if (err.status == 204):
            if (debug):
                print("[DEBUG: createAndGetObjectStoreBucket]: O bucket existe e retornou 204. Entao ele existe e eh nosso (I hope so)...")
    if (debug):
        print(
            "[DEBUG: createAndGetObjectStoreBucket]: Vamos ver se e o nosso")
    try:
        osb = objectStoreConnection.get_bucket(objectStoreDict['BUCKET'])
    except S3ResponseError as err:
        if (err.status == 204):
            pass
        else:
            raise
    k = Key(osb)
    k.key = '001-DoNotTouchMe-001'
    try:
        conteudo = k.get_contents_as_string()
    except S3ResponseError as err:
        if (debug):
            print(
                "[DEBUG: createAndGetObjectStoreBucket]: Ta dificil... Tem um bucket com o nome %s que nao eh nosso. Melhor parar isso aqui" % (
                    objectStoreDict['BUCKET']))
            print(err)
        sys.exit("Precisa limpar os buckets. Tem bucket com conteudo improprio")
    if (conteudo != "sim"):
        if (debug):
            print(
                "[DEBUG: createAndGetObjectStoreBucket]: Ta dificil... Tem um bucket com o nome %s que nao eh nosso. Melhor parar isso aqui" % (
                    objectStoreDict['BUCKET']))
        sys.exit("Precisa limpar os buckets. Tem bucket com conteudo improprio")
    else:
        if (debug):
            print("[DEBUG: createAndGetObjectStoreBucket]: Otimo! O bucket %s esta aqui e eh nosso" % (
                objectStoreDict['BUCKET']))
    return (osb)
