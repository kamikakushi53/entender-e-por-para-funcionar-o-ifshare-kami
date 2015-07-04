from __future__ import print_function
import sys
import StringIO
import random
import json
import mysql.connector

from flask import Flask, request, render_template, abort, send_file, url_for
from werkzeug.routing import BaseConverter
from utilities import createAndGetObjectStoreBucket, getObjectStoreConnection, writeMetadata, bestSize

from config_file import *

__author__ = 'dclobato'

if (CONFIG['storage']['numcopies'] > CONFIG['storage']['numsites']):
    print("Numero de copias maior que o numero de sites de armazenamento!")
    sys.exit("Erro de configuracao")


class RegexConverter(BaseConverter):
    def __init__(self, url_map, *items):
        super(RegexConverter, self).__init__(url_map)
        self.regex = items[0]


app = Flask(__name__)
app.url_map.converters['regex'] = RegexConverter
app.debug = True


@app.route('/')
def index():
    """ Quem somos e o que fazemos """
    return "<p>Bem vindo ao tracker do IFShare, implementado em Python com Flask</p> \
    	   <p>Ha duas formas de invocar esse servico (<b>ATENCAO QUE MUDOU!</b>):\
	   <ul><li>http://localhost:8008/URLbySHA/sha1arquivo/sha1chunk?n=numero_de_links_para_download&t=timeout_das_urls_geradas_em_segundos</li>\
	   <li>http://localhost:8008/URLbyChunk/sha1arquivo/numero_do_chunk?n=numero_de_links_para_download&t=timeout_das_urls_geradas_em_segundos</li>" \
           "<li>http://localhost:8008/ShowAll</li>" \
           "<p>Os parametros n e t sao opcionais. Se forem omitidos, serao preenchidos com os valores default pelo servidor</p>"


def getURLsFromQuery(filtro, RESULT, numlinks, timeout):
    db = mysql.connector.connect(user=CONFIG['DB']['USER'],
                                 password=CONFIG['DB']['PASS'],
                                 host=CONFIG['DB']['ENDPOINT'],
                                 database=CONFIG['DB']['DATABASE'])
    cursor = db.cursor()
    query = "SELECT sha1arquivo, chunk, sha1parte, onde FROM partes WHERE sha1arquivo = '" + RESULT['request']['a'] + "' AND " + filtro
    cursor.execute(query)
    rs = cursor.fetchall()
    cursor.close()
    db.close()

    RESULT['STATUS'] = 200
    if (len(rs) < numlinks):
        RESULT['STATUS'] = 206
        numlinks = len(rs)
    if (len(rs) > 0):
        if (timeout > CONFIG['retrieve']['timeout']):
            RESULT['reply']['timeout'] = CONFIG['retrieve']['timeout']
        else:
            RESULT['reply']['timeout'] = timeout
        URLs = list()
        for row in rs:
            filename = str(row[0]) + "." + str(row[2]) + ".ifsharechunk"
            tmpKey = upBucket[int(row[3])].get_key(filename)
            if tmpKey is None:
                RESULT['STATUS'] = 206
                numlinks -= 1
            else:
                tmpUrl = tmpKey.generate_url(RESULT['reply']['timeout'], query_auth=True, force_http=True)
                URLs.append(tmpUrl)

        random.shuffle(URLs)
        for i in range(numlinks):
            RESULT['reply'][i] = str(URLs[i])
        RESULT['reply']['numlinks'] = i + 1
    else:
        RESULT['STATUS'] = 404
        RESULT['reply']['numlinks'] = 0


@app.route('/URLbySHA/<regex("[a-f\d]{40}"):sha1arquivo>/<regex("[a-f\d]{40}"):sha1parte>')
def urlbysha(sha1arquivo, sha1parte):
    RESULT = Tree()
    RESULT['OPCODE'] = "URLbySHA"

    numlinks = int(request.args.get('n', default=CONFIG['retrieve']['numlink']))
    timeout = int(request.args.get('t', default=CONFIG['retrieve']['timeout']))

    RESULT['request']['a'] = str(sha1arquivo)
    RESULT['request']['s'] = str(sha1parte)
    RESULT['request']['n'] = numlinks
    RESULT['request']['t'] = timeout

    getURLsFromQuery("sha1parte = '" + sha1parte + "';", RESULT, numlinks, timeout)

    return (json.dumps(RESULT, sort_keys=True, indent=2))


@app.route('/URLbyChunk/<regex("[a-f\d]{40}"):sha1arquivo>/<int:chunk>')
def urlbychunk(sha1arquivo, chunk):
    RESULT = Tree()
    RESULT['OPCODE'] = "URLbyChunk"

    numlinks = int(request.args.get('n', default=CONFIG['retrieve']['numlink']))
    timeout = int(request.args.get('t', default=CONFIG['retrieve']['timeout']))

    RESULT['request']['a'] = str(sha1arquivo)
    RESULT['request']['c'] = int(chunk)
    RESULT['request']['n'] = numlinks
    RESULT['request']['t'] = timeout

    getURLsFromQuery("chunk=" + str(chunk) + ";", RESULT, numlinks, timeout)

    return (json.dumps(RESULT, sort_keys=True, indent=2))


@app.route('/ShowAll')
def showall():
    db = mysql.connector.connect(user=CONFIG['DB']['USER'],
                                 password=CONFIG['DB']['PASS'],
                                 host=CONFIG['DB']['ENDPOINT'],
                                 database=CONFIG['DB']['DATABASE'])
    cursor = db.cursor()
    query = "SELECT sha1arquivo, nome, tamanho, datahora FROM arquivos ORDER BY nome;"
    cursor.execute(query)
    rs = cursor.fetchall()
    cursor.close()
    db.close()

    output = render_template('listaarquivos.html', rows=rs)
    return output


@app.route('/CheckHealth/<regex("[a-f\d]{40}"):sha1arquivo>')
def checkhealth(sha1arquivo):
    db = mysql.connector.connect(user=CONFIG['DB']['USER'],
                                 password=CONFIG['DB']['PASS'],
                                 host=CONFIG['DB']['ENDPOINT'],
                                 database=CONFIG['DB']['DATABASE'])
    cursor = db.cursor()
    query = "SELECT sha1arquivo, nome, tamanho, datahora, chunksize, numchunks FROM arquivos WHERE sha1arquivo = '" + sha1arquivo + "';"
    cursor.execute(query)
    rs = cursor.fetchall()
    if (len(rs) == 0):
        abort(404)
    cursor.close()

    ifshare_propriedades = dict()
    ifshare_propriedades['hashgeral'] = str(rs[0][0])
    ifshare_propriedades['arquivo'] = str(rs[0][1])
    ifshare_propriedades['tamanho'] = int(rs[0][2])
    ifshare_propriedades['criacao_do_arquivo'] = rs[0][3]
    ifshare_propriedades['chunksize'] = int(rs[0][4])
    ifshare_propriedades['numchunks'] = int(rs[0][5])

    listaProvedores = []
    for i in range (CONFIG['storage']['numsites']):
        listaProvedores.append(CONFIG['storage'][i]['PROVIDER'])

    chunkStatus = []
    cursor = db.cursor()
    for i in range (ifshare_propriedades['numchunks']):
        query = "SELECT sha1arquivo, sha1parte, chunk, onde FROM partes WHERE sha1arquivo = '" + sha1arquivo + "' AND chunk = " + str(i)
        cursor.execute(query)
        rs = cursor.fetchall()
        cores = ["lightgray"] * (CONFIG['storage']['numsites']+2)
        cores[0] = i
        for row in rs:
            cores[1] = str(row[1])
            filename = str(row[0]) + "." + str(row[1]) + ".ifsharechunk"
            if upBucket[int(row[3])].get_key(filename) is None:
                cores[int(row[3])+2] = "red"
            else:
                cores[int(row[3])+2] = "green"
        chunkStatus.append(cores)

    db.close()

    output = render_template('filehealth.html', file=ifshare_propriedades, qtos=CONFIG['storage']['numsites'], chunks=chunkStatus, lista=listaProvedores)
    return output


@app.route('/Get/<regex("[a-f\d]{40}"):sha1arquivo>')
def getifshare(sha1arquivo):
    db = mysql.connector.connect(user=CONFIG['DB']['USER'],
                                 password=CONFIG['DB']['PASS'],
                                 host=CONFIG['DB']['ENDPOINT'],
                                 database=CONFIG['DB']['DATABASE'])
    cursor = db.cursor()
    query = "SELECT sha1arquivo, nome, tamanho, datahora, chunksize, numchunks FROM arquivos WHERE sha1arquivo = '" + sha1arquivo + "';"
    cursor.execute(query)
    rs = cursor.fetchall()
    if (len(rs) == 0):
        abort(404)

    ifshare_propriedades = dict()
    ifshare_propriedades['hashgeral'] = str(rs[0][0])
    ifshare_propriedades['arquivo'] = str(rs[0][1])
    ifshare_propriedades['tamanho'] = int(rs[0][2])
    ifshare_propriedades['criacao_do_arquivo'] = rs[0][3]
    ifshare_propriedades['chunksize'] = int(rs[0][4])
    ifshare_propriedades['numchunks'] = int(rs[0][5])

    query = "SELECT DISTINCT sha1arquivo, sha1parte, chunk FROM partes WHERE sha1arquivo = '" + sha1arquivo + "' ORDER BY chunk;"
    cursor.execute(query)
    rs = cursor.fetchall()
    cursor.close()
    db.close()
    i = 0
    for row in rs:
        ifshare_propriedades['hashchunk' + str(i)] = str(row[1])
        i = i + 1

    s = StringIO.StringIO()
    writeMetadata(s, ifshare_propriedades)
    s.seek(0, 0)

    return send_file(s, attachment_filename=ifshare_propriedades['arquivo'] + ".ifshare", as_attachment=True)


@app.context_processor
def my_utility_processor():
    def betsSizeTemplate(tamanho):
        return bestSize(tamanho)

    return dict(bestSizeTemplate=bestSize)


@app.errorhandler(404)
def error404(error):
    return ("Houve mudanca na forma de chamar o servico. Veja a pagina principal para conferir a nova sintaxe.")


print("")
print("-" * 80)
print("Estabelecendo conexoes iniciais")
print("-" * 80)

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
print("Atendendo pedidos")
print("-" * 80)

app.run(host="0.0.0.0", port=8008, use_reloader=True)

print("")
print("-" * 80)
print("Encerrando as conexoes")
print("-" * 80)
if (CONFIG['DOBUCKETUPLOAD']):
    print("Encerrando conexao com os buckets do provedores de armazenamento")
    for i in range(CONFIG['storage']['numsites']):
        print("   %s" % (CONFIG['storage'][i]['PROVIDER']))
        s3Upload[i].close()
print("=-" * 40)
print("Fim dos trabalhos!")
print("=-" * 40)
sys.exit()
