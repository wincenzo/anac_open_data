
from itertools import islice
from collections import defaultdict
from datetime import datetime
import errno
import json
import logging
import os

import mysql.connector as connector
from mysql.connector import errors

from .statements import *


class DataBase:
    def __init__(self, host, database, user, password):
        self.credentials = {'host': host,
                            'database': database,
                            'user': user,
                            'password': password}

    def connect(self):
        self.cnx = connector.connect(**self.credentials,
                                     buffered=True)

    def close(self): self.cnx.close()

    def execute_many(self, query, params=()):
        with self.cnx.cursor() as cur:
            cur.executemany(query, params)
            self.cnx.commit()

            return cur

    def execute(self, query, params=[]):
        with connector.connect(**self.credentials,
                               buffered=True,
                               raise_on_warnings=True) as cnx:
            with cnx.cursor(dictionary=True) as cur:
                cur.execute(query, params)
                cnx.commit()

                return cur


class Operations:
    def __init__(self, database, downdir):
        self.database = database
        self.downdir = downdir
        self.db_name = self.database.credentials['database']
        self.columns = self.get_all_columns()

    def get_all_columns(self, query=GET_ALL_COLUMNS):
        '''
        Ritorna un indice di tutte le colonne per ogni tabella del db.
        '''
        result = self.database.execute(query)
        result = [(row['TABLE_NAME'], row['COLUMN_NAME'])
                  for row in result]

        columns = defaultdict(list)
        for k, v in result:
            columns[k].append(v)

        return columns

    def gen_row(self, filepath, refcols):
        '''
        Generatore che ritorna una linea dai file json convertendola in
        dizionario e selezionando solo le colonne presenti anche nel
        database (a volte nei files vengono aggiunte delle nuove colonne
        non presenti nel db o con nomi differenti). Inoltre assicura che 
        i campi vuoti siano avvalorati correttamente e che i valori nulli
        siano istanziati come "None" in modo che il connettore li traduca 
        in NULL durante l'inserimento.
        '''
        def fix(row):
            select = {}
            for k in sorted(row, key=len):
                _k = k.replace('-', '_')
                if refcols:
                    for col in sorted(refcols, reverse=True, key=len):
                        if (_k.lower().startswith(col.lower())
                                and col not in select):
                            select[col] = row.pop(k) or None
                            break
                else:
                    select[_k] = row.pop(k) or None

            return select

        with open(filepath) as file:
            yield from (json.loads(row, object_hook=fix) for row in file)

    @staticmethod
    def batched_rows(generator, n):
        '''
        Il generatore crea pacchetti di linee da inserire nel db usando 
        il metodo .executemany() previsto dal connettore mysql
        '''
        while (batch := tuple(islice(generator, n))):
            yield batch

    def creator(self,
                stmts,
                params=[],
                hash=False,
                key=True):
        '''
        Crea le tabelle qualora non siano già presenti nel db.Eventualmente 
        aggiunge "id" primary key ed "hash" unique key.
        '''
        for k in stmts:
            try:
                self.database.execute(stmts[k], params)
                
                k = k.replace('-', '_')
                columns = self.get_all_columns()[k]

                if hash:
                    columns = ','.join(columns)
                    hash_stmt = HASH_KEY.format(k, k, columns)
                    self.database.execute(hash_stmt)

                if key:
                    pk_stmt = ADD_ID.format(k, k)
                    self.database.execute(pk_stmt)

            except errors.DatabaseError as w:
                logging.warning(f'{w}')

            else:
                logging.info(f'CREATED: "{k}"')

            finally:
                self.columns = self.get_all_columns()

        logging.info('CREATE: COMPLETED')

    def format_insert(self, file_path, table_name):
        '''
        Nel caso il file non sia vuoto (sono presenti file vuoti), formatta 
        dinamicamente l'insert statement per ogni tabella.
        '''
        if os.stat(file_path).st_size > 0:
            columns = self.columns[table_name]

            values = [f'%({c})s' for c in columns]
            values = ','.join(values)

            columns = ','.join(columns)

        else:
            columns = None
            values = None

        insert_stmt = INSERT_TABLES.format(table_name, columns, values)

        return insert_stmt

    def insert(self,
               file,
               table,
               batch_size):
        '''
        Esegue l'insert nel db. 
        '''
        insert_stmt = self.format_insert(file.path, table.name)

        n_rows = 0

        refcols = self.columns[table.name]
        reader = self.gen_row(file.path, refcols)
        batches = self.batched_rows(reader, batch_size)
        for batch in batches:
            n_rows += self.database.execute_many(
                insert_stmt, batch).rowcount

        return n_rows

    def insert_sintesi(self):
        '''
        Inserisce i dati nella tabella "sintesi" verificando che non siano già 
        stati inseriti in precedenza confrontando la data di inserimento.
        '''
        columns = self.columns['sintesi']
        columns = ','.join(columns)

        last_ins = self.database.execute(LAST_LOAD).fetchone()
        last_ins = last_ins['last_ins'] or datetime(1990, 1, 1)

        logging.info(f'INSERT: "sintesi"')

        insert_sintesi = INSERT_SINTESI.format(columns)
        params = (last_ins, last_ins, last_ins, RGX_DENOMINAZIONE)
        rows = self.database.execute(insert_sintesi, params).rowcount

        logging.info(f'DONE: {rows} new rows inserted')

        return rows

    def loader(self,
               batch_size=BATCH_SIZE,
               directories=[],
               files=[],
               tables=[],
               clean=False):
        '''
        Gestisce l'inserimento scorrendo la directory di download. Permette 
        di operare solo su specifiche directory o anche solo su file. Verifica 
        se nella tabella "loaded" (log dei file inseriti) siano già presenti i 
        file da inserire per evitare che vengano inseriti nuovamente generando 
        duplicati, eventualmente aggiorna la tabella. Permette anche di cancellare 
        i file dopo l'inserimento.
        '''
        if files:
            for f in files:
                if not os.path.isfile(f):
                    raise FileNotFoundError(
                        errno.ENOENT, os.strerror(errno.ENOENT), f)

        if directories:
            for d in directories:
                if not os.path.isdir(d):
                    raise FileNotFoundError(
                        errno.ENOENT, os.strerror(errno.ENOENT), d)

        if tables:
            for tab in tables:
                assert tab in self.columns, f'table "{tab}" not in database schema'

        self.database.connect()

        for tab in os.scandir(self.downdir):
            tot_rows = 0
            if not tables or tab.name in tables:
                for dir in os.scandir(tab):
                    if not directories or dir.path in directories:
                        for f in os.scandir(dir):
                            n_rows = 0
                            if not files or f.path in files:
                                loaded = self.database.execute(
                                    GET_LOADED, [tab.name])

                                loaded = any(filter(
                                    lambda x: x['file_name'] == f.name, loaded))
                                if not loaded:
                                    logging.info(
                                        f'INSERT: "{f.name}" into "{tab.name}"')
                                    try:
                                        n_rows = self.insert(
                                            f, tab, batch_size)

                                    except Exception as e:
                                        logging.exception(f'{e}')

                                    else:
                                        tot_rows += n_rows

                                        self.database.execute_many(
                                            INSERT_LOADED, [(tab.name, f.name)])

                                        if clean and f.name not in ('cpv_tree.json',
                                                                    'province.json'):
                                            try:
                                                os.remove(f.path)
                                                logging.info(
                                                    f'"{f.path}" deleted')

                                            except FileNotFoundError:
                                                ...

                                else:
                                    logging.warning(
                                        f'"{f.name}" already loaded')

            if tot_rows > 0:
                logging.info(
                    f'DONE: {tot_rows} row inserted into "{tab.name}"')

        self.database.close()

        logging.info('INSERT: COMPLETED')
