import hashlib
import os
import socket
import sys
import random
import urllib2
import json
import bitstring
from progressbar import ProgressBar, Percentage, Bar, ETA

from utilities import readMetadata

__author__ = 'dclobato'

MAXCONNECTIONERRORS = 20
MAXTRACKERERRORS = 10
MAXCHECKSUMERRORS = 5

def showDownloadStatus(bitmap, d, ce, te, cse):
    print("============================================================")
    print("Chunks obtidos.: %d/%d" % (bitmap.count(1), d['numchunks']))
    print("Bitmap.........: [%s]" % bitmap.bin)
    print("")
    print("Erros conexao..: %02d   tracker: %02d   checksum: %02d" % (ce, te, cse))
    print("============================================================")
    return


def writeChunkOnFile(fd, data, chunk, d):
    offset = chunk * d['chunksize']
    fd.seek(offset, 0)
    fd.write(data)
    return


arquivo_raw = raw_input("Qual o arquivo IFShare? ")
(drive, caminho) = os.path.splitdrive(arquivo_raw)
(caminho, filename) = os.path.split(caminho)

ifshare_propriedades = dict()

try:
    fd = open(arquivo_raw, "rb")
except:
    sys.exit("Problemas para acesso ao arquivo")

readMetadata(fd, ifshare_propriedades)
fd.close()
bitMap = bitstring.BitArray(ifshare_propriedades['numchunks'])

filename = drive + caminho + ifshare_propriedades['arquivo']
try:
    fd_output = open(filename, 'r+b')
except IOError:
    fd_output = open(filename, 'wb')
    fd_output.close()
    fd_output = open(filename, 'r+b')

fd_output.truncate(ifshare_propriedades['tamanho'])
connectionErrors = 0
checksumErrors = 0
trackErrors = 0

widgets = ["Verificando arquivo ", Percentage(), " ", Bar(marker='#', left='[', right=']'), " ", ETA()]

pbar = ProgressBar(widgets=widgets, maxval=ifshare_propriedades['numchunks'], term_width=80)
pbar.start()

for i in range(ifshare_propriedades['numchunks']):
    fd_output.seek(i * ifshare_propriedades['chunksize'])
    hasher = hashlib.sha1()
    buf = fd_output.read(ifshare_propriedades['chunksize'])
    hasher.update(buf)
    chunkHash = hasher.hexdigest()
    if (ifshare_propriedades['hashchunk' + str(i)] == chunkHash):
        bitMap.set(1, i)
    else:
        bitMap.set(0, i)
    pbar.update(i)

pbar.finish()
server = "http://tracker.lab-rsd.com:8008"
service = "URLbyChunk"
args = "n=2&t=360"

while (bitMap.count(0) > 0) and (connectionErrors < MAXCONNECTIONERRORS) and (checksumErrors < MAXCHECKSUMERRORS) and (
            trackErrors < MAXTRACKERERRORS):
    showDownloadStatus(bitMap, ifshare_propriedades, connectionErrors, trackErrors, checksumErrors)
    chunkToGet = random.choice(list(bitMap.findall(bitstring.Bits(bin="0b0"))))
    print ("Obtendo chunk %d..." % chunkToGet)
    URL = server + "/" + service + "/" + ifshare_propriedades['hashgeral'] + "/" + str(chunkToGet) + "?" + args
    req = urllib2.Request(URL)
    opener = urllib2.build_opener()
    try:
        REPLY = json.loads(str(opener.open(req, timeout=10).read()), "latin1")
    except (urllib2.URLError, socket.timeout) as e:
        print("Timeout para acesso ao tracker.")
        connectionErrors += 1
        continue
    except Exception as e:
        print("Tracker inacessivel. Tentando novamente...")
        print(e.reason)
        connectionErrors += 1
        continue

    if ((REPLY['STATUS'] != 200) and (REPLY['STATUS'] != 206)):
        print ("Algum problema na recuperacao. Status %d" % REPLY['STATUS'])
        trackErrors += 1
        continue

    print("Pedimos %d e obtivemos %d opcoes de download" % (REPLY['request']['n'], REPLY['reply']['numlinks']))
    if (REPLY['reply']['numlinks'] == 0):
        print("Impossivel seguir em frente: chunk %d nao tem replicas disponiveis" % (chunkToGet))
        break

    for downloadTry in range(REPLY['reply']['numlinks']):
        print("Tentando o link #%d" % (downloadTry))
        req = urllib2.Request(str(REPLY['reply'][str(downloadTry)]))
        try:
            hasher = hashlib.sha1()
            chunkData = urllib2.build_opener().open(req, timeout=10).read()
        except (urllib2.URLError, socket.timeout) as e:
            print("Timeout no acesso ao link #%d" % (downloadTry))
            if (downloadTry == REPLY['reply']['numlinks']-1):
                print("Tentamos todos os links oferecidos pelo tracker e nada feito. Passando para o proximo chunk e deixando esse para depois")
            connectionErrors += 1
        except Exception as e:
            print("Link #%d indisponivel." % (downloadTry))
            if (downloadTry == REPLY['reply']['numlinks'] - 1):
                print("Tentamos todos os links oferecidos pelo tracker e nada feito. Passando para o proximo chunk e deixando esse para depois")
            connectionErrors += 1
        else:
            hasher.update(chunkData)
            chunkHash = hasher.hexdigest()

            if (ifshare_propriedades['hashchunk' + str(chunkToGet)] == chunkHash):
                print("Parte recuperada com sucesso!")
                writeChunkOnFile(fd_output, chunkData, chunkToGet, ifshare_propriedades)
                bitMap.set(1, chunkToGet)
                break
            else:
                print("Erro no hash da parte recuperada. Esperado [%s]. Recuperado [%s]." % (
                    ifshare_propriedades['hashchunk' + str(chunkToGet)], chunkHash))
                checksumErrors += 1

fd_output.close()

showDownloadStatus(bitMap, ifshare_propriedades, connectionErrors, trackErrors, checksumErrors)

if (bitMap.count(0) == 0):
    print("Arquivo recuperado com sucesso")
elif (connectionErrors >= MAXCONNECTIONERRORS):
    print("Download interrompido por excesso de erros de conexao")
elif (trackErrors >= MAXTRACKERERRORS):
    print("Download interrompido por excesso de erros do tracker")
elif (checksumErrors >= MAXCHECKSUMERRORS):
    print("Download interrompido por excesso de erros de checksum")
else:
    print("Downloado interrompido")
