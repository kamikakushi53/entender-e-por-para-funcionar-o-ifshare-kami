from __future__ import print_function
import boto
from progressbar import ETA, ProgressBar, Bar
from utilities import getObjectStoreConnection

__author__ = 'dclobato'

import mysql.connector
import boto
import boto.sqs
import boto.sqs.message

from config_file import *

print("")
print("-" * 80)
print("Limpando as coisas")
print("-" * 80)
print("Removendo a fila de arquivos a processar...")
sqsConnection = boto.sqs.connect_to_region(CONFIG['Incoming']['REGION'],
                                           aws_access_key_id=CONFIG['Incoming']['ACCESS_KEY'],
                                           aws_secret_access_key=CONFIG['Incoming']['SECRET_KEY'])
myQueue = sqsConnection.lookup(CONFIG['Incoming']['QUEUE'])
if myQueue != None:
    sqsConnection.delete_queue(myQueue)
sqsConnection.close()

print("Removendo o bucket de arquivos a processar...")
s3Incoming = getObjectStoreConnection(CONFIG['Incoming'])
bucket = s3Incoming.lookup(CONFIG['Incoming']['BUCKET'])
if bucket != None:
    for k in bucket.list():
        print(".", end='')
        k.delete()
    print("")
    s3Incoming.delete_bucket(CONFIG['Incoming']['BUCKET'])
s3Incoming.close()

print("Removendo os chunks e buckets nos provedores de armazenamento...")
for i in range(CONFIG['storage']['numsites']):
    tmpS3Connection = getObjectStoreConnection(CONFIG['storage'][i], debug=False)
    bucket = tmpS3Connection.lookup(str(CONFIG['storage'][i]['BUCKET']))
    if bucket != None:
        widgets = ["   ", str(CONFIG['storage'][i]['PROVIDER']), " ", Bar(marker='#', left='[', right=']'), " ", ETA()]
        numKeys = len(list(bucket.list()))
        if (numKeys > 0):
            pbar = ProgressBar(widgets=widgets, maxval=numKeys, term_width=80)
            pbar.start()
            i=0
            for k in bucket.list():
                pbar.update(i)
                k.delete()
                i+=1
            pbar.finish()
        tmpS3Connection.delete_bucket(str(CONFIG['storage'][i]['BUCKET']))
    tmpS3Connection.close()

print("Limpando o banco de dados...")
db = mysql.connector.connect(user=CONFIG['DB']['USER'],
                             password=CONFIG['DB']['PASS'],
                             host=CONFIG['DB']['ENDPOINT'],
                             database=CONFIG['DB']['DATABASE'])
cursor = db.cursor()
query = "TRUNCATE TABLE arquivos;"
cursor.execute(query)
db.commit()
query = "TRUNCATE TABLE partes;"
cursor.execute(query)
db.commit()
db.close()
