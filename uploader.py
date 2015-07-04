__author__ = 'dclobato'
import datetime
import hashlib
import math
import os

import boto
import boto.sqs
import boto.sqs.message
import filechunkio

from progressbar import ProgressBar, Bar, ETA

from utilities import writeMetadata, getObjectStoreConnection, createAndGetObjectStoreBucket, showMetadata
from config_file import *


class MultipartUploader(object):
    def __init__(self, filename, filesize, keyname, bucket, message="Subindo arquivo"):
        self.filename = filename
        self.filesize = filesize
        self.keyname = keyname
        self.bucket = bucket
        self.message = message
        self.pbar = None

    def _dl_progress_cb(self, trans, total):
        self.pbar.update(trans)

    def put(self):
        multipartupload = self.bucket.initiate_multipart_upload(self.keyname)
        chunk_count = int(math.ceil(self.filesize / CONFIG['CHUNKUP']))

        widgets = [self.message, " ", Bar(marker='#', left='[', right=']'), " ", ETA()]

        self.pbar = ProgressBar(widgets=widgets, maxval=self.filesize, term_width=80)

        self.pbar.start()
        for i in range(chunk_count + 1):
            offset = CONFIG['CHUNKUP'] * i
            bytes = min(CONFIG['CHUNKUP'], self.filesize - offset)
            with filechunkio.FileChunkIO(self.filename, 'rb', offset=offset, bytes=bytes) as fp:
                multipartupload.upload_part_from_file(fp, part_num=i + 1, cb=self._dl_progress_cb, num_cb=100)

        self.pbar.finish()
        multipartupload.complete_upload()


ifshare_propriedades = dict()

arquivo_raw = raw_input("Qual o arquivo que vamos subir? ")
ifshare_propriedades['chunksize'] = int(raw_input("Qual o tamanho do chunk? "))

(drive, caminho) = os.path.splitdrive(arquivo_raw)
(caminho, arquivo) = os.path.split(caminho)

ifshare_propriedades['arquivo'] = arquivo
arquivo_saida = arquivo_raw + ".ifshare"

try:
    ifshare_propriedades['tamanho'] = os.path.getsize(arquivo_raw)
    ifshare_propriedades['criacao_do_arquivo'] = datetime.datetime.fromtimestamp(os.path.getmtime(arquivo_raw))
except:
    print ("Problemas para acesso ao arquivo...")
    exit()

total_de_chunks = int(math.ceil(ifshare_propriedades['tamanho'] / float(ifshare_propriedades['chunksize'])))
ifshare_propriedades['numchunks'] = total_de_chunks

fd_input = open(arquivo_raw, 'rb')
hashergeral = hashlib.sha1()
for chunk in range(0, total_de_chunks):
    hasher = hashlib.sha1()
    buf = fd_input.read(ifshare_propriedades['chunksize'])
    hashergeral.update(buf)
    hasher.update(buf)
    ifshare_propriedades['hashchunk' + str(chunk)] = hasher.hexdigest()

ifshare_propriedades['hashgeral'] = hashergeral.hexdigest()
fd_input.close()

showMetadata(ifshare_propriedades)

fd_output = open(arquivo_saida, "w")
writeMetadata(fd_output, ifshare_propriedades)
fd_output.close()

s3connection = getObjectStoreConnection(CONFIG['Incoming'], debug=False)
bucket = createAndGetObjectStoreBucket(CONFIG['Incoming'], s3connection, debug=False)

uploader = MultipartUploader(arquivo_raw,
                             ifshare_propriedades['tamanho'],
                             ifshare_propriedades['arquivo'],
                             bucket)
uploader.put()

uploader = MultipartUploader(arquivo_raw + ".ifshare",
                             os.path.getsize(arquivo_raw + ".ifshare"),
                             ifshare_propriedades['arquivo'] + ".ifshare",
                             bucket,
                             message="Subindo metadados")
uploader.put()

s3connection.close()

print("Arquivos no bucket. Atualizando fila...")

sqsConnection = boto.sqs.connect_to_region(CONFIG['Incoming']['REGION'],
                                           aws_access_key_id=CONFIG['Incoming']['ACCESS_KEY'],
                                           aws_secret_access_key=CONFIG['Incoming']['SECRET_KEY'])

myQueue = sqsConnection.lookup(CONFIG['Incoming']['QUEUE'])
if myQueue is None:
    print("Criando fila de arquivos a processar...")
    myQueue = sqsConnection.create_queue(CONFIG['Incoming']['QUEUE'])
else:
    print("Fila de arquivos a processar ja existe")

message = boto.sqs.message.Message()
message.set_body(ifshare_propriedades['arquivo'] + '.ifshare')
myQueue.write(message)
sqsConnection.close()
