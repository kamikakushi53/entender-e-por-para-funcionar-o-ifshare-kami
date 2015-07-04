from collections import defaultdict

Tree = lambda: defaultdict(Tree)

CONFIG = Tree()
CONFIG['Incoming']['PROVIDER'] = "AWS South America"
CONFIG['Incoming']['ACCESS_KEY'] = "kkkkkkkkkkkkkk"
CONFIG['Incoming']['SECRET_KEY'] = "kkkkkkkkkkkkkkkkkkkkkkkkkkkk"
CONFIG['Incoming']['BUCKET'] = "br.edu.ifsp.ctd.incoming"
CONFIG['Incoming']['REGION'] = "sa-east-1"
CONFIG['Incoming']['QUEUE'] = "br-edu-ifsp-ctd-tracker"
CONFIG['Incoming']['CT'] = "region"

CONFIG['storage']['numsites'] = 5                # Quantos repositorios de armazenamento existem no total
CONFIG['storage']['numcopies'] = 3               # Cada chunk sera replicado em quantos repositorios

CONFIG['retrieve']['timeout'] = 2 * 60 * 60      # 2 horas de validade para o link de recuperacao dos objetos (MAX PERMITIDO AQUI)
CONFIG['retrieve']['numlink'] = 3                # numero de links para cada parte que serao retornados para o usuario

CONFIG['storage'][0]['PROVIDER'] = "DreamHost"
CONFIG['storage'][0]['ACCESS_KEY'] = "xxxxxxxx"
CONFIG['storage'][0]['SECRET_KEY'] = "xxxxxxxxxxxxxxxxxx"
CONFIG['storage'][0]['BUCKET'] = "br.edu.ifsp.ctd.repo1"
CONFIG['storage'][0]['HOST'] = "objects.dreamhost.com"
CONFIG['storage'][0]['CT'] = "host"

CONFIG['storage'][1]['PROVIDER'] = "AWS South America"
CONFIG['storage'][1]['ACCESS_KEY'] = "xxxxxxxx"
CONFIG['storage'][1]['SECRET_KEY'] = "xxxxxxxxxxxxxxxxxx"
CONFIG['storage'][1]['BUCKET'] = "br.edu.ifsp.ctd.repo2"
CONFIG['storage'][1]['REGION'] = "sa-east-1"
CONFIG['storage'][1]['CT'] = "region"

CONFIG['storage'][2]['PROVIDER'] = "AWS US West Oregon"
CONFIG['storage'][2]['ACCESS_KEY'] = "xxxxxxxx"
CONFIG['storage'][2]['SECRET_KEY'] = "xxxxxxxxxxxxxxxxxx"
CONFIG['storage'][2]['BUCKET'] = "br.edu.ifsp.ctd.repo3"
CONFIG['storage'][2]['REGION'] = "us-west-2"
CONFIG['storage'][2]['CT'] = "region"

CONFIG['storage'][3]['PROVIDER'] = "AWS EU Ireland"
CONFIG['storage'][3]['ACCESS_KEY'] = "xxxxxxxx"
CONFIG['storage'][3]['SECRET_KEY'] = "xxxxxxxxxxxxxxxxxx"
CONFIG['storage'][3]['BUCKET'] = "br.edu.ifsp.ctd.repo4"
CONFIG['storage'][3]['REGION'] = "eu-west-1"
CONFIG['storage'][3]['CT'] = "region"

CONFIG['storage'][4]['PROVIDER'] = "Google Cloud"
CONFIG['storage'][4]['ACCESS_KEY'] = "xxxxxxxx"
CONFIG['storage'][4]['SECRET_KEY'] = "xxxxxxxxxxxxxxxxxx"
CONFIG['storage'][4]['BUCKET'] = "br-edu-ifsp-ctd-repo5"
CONFIG['storage'][4]['HOST'] = "storage.googleapis.com"
CONFIG['storage'][4]['CT'] = "host"

CONFIG['DB']['INSTANCE'] = "ifsptracker"
CONFIG['DB']['ENDPOINT'] = "localhost"
CONFIG['DB']['DATABASE'] = "tracker"
CONFIG['DB']['USER'] = "chopper"
CONFIG['DB']['PASS'] = "nnnnnnnnnnnnn"
CONFIG['DB']['ADMINUSER'] = "awsuser"
CONFIG['DB']['ADMINPASSWD'] = "nnnnnnnnnnnnnnnn"

CONFIG['DOREMOVE'] = True
CONFIG['DOREMOVELOCAL'] = True
CONFIG['DOBDINSERT'] = True
CONFIG['DOBUCKETUPLOAD'] = True

CONFIG['CHUNKUP'] = 5*1024*1024

