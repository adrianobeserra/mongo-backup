import subprocess
import json
from datetime import datetime
from pymongo import MongoClient
import os.path
from bson import json_util

DB_NAME = "backupneurobots"
DB_HOST = "ds135486.mlab.com"
DB_PORT = 35486
DB_USER = "neurobots"
DB_PASS = "neurob0ts@@"

connection = MongoClient(DB_HOST, DB_PORT)
db = connection[DB_NAME]
db.authenticate(DB_USER, DB_PASS)
path_mongoexport = r'C:\Program Files\mongoDB\Server\4.0\bin\mongoexport'
path_mongoimport = r'C:\Program Files\mongoDB\Server\4.0\bin\mongoimport'
path_target = r'c:\backup\mongosql.json'
collection = 'cliente_collection'
db = 'test'


def backup():
    # Backup do mongo para arquivo
    p = subprocess.Popen([path_mongoexport, "--db", db, "--collection",
                      collection, "--pretty", "--out", path_target], stdout=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()  

    # Aguardar o comando ser concluído
    p_status = p.wait()


    # Atualização do conteúdo do json
    if os.path.isfile(path_target):
        with open(path_target) as f:
            file_backup = json.load(f)

        backup = {}
        backup['cliente'] = 'cliente_teste'
        backup['data'] = str(datetime.now())
        backup['conteudo'] = file_backup
        
        with open(path_target, 'w') as f:
            f.write(json.dumps(backup))

        # Envio do backup para o MLab
        p = subprocess.Popen([path_mongoimport, "-h", "ds135486.mlab.com:35486", "-d", DB_NAME, "-u", DB_USER,
                          "-p", DB_PASS, "--file", path_target, "--collection", "backup", "--type", "json"])
        (output, err) = p.communicate()  

        # Aguardar o comando ser concluído
        p_status = p.wait()
        
        # Remove o arquivo de backup já enviado
        os.remove(path_target)
    print('Feito!')

backup()
