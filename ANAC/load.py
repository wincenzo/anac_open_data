
from itertools import islice
from collections import defaultdict
from datetime import datetime
import errno
import json
import logging
import os

import mysql.connector as connector
from mysql.connector import errors
from mysql.connector import errorcode

from .statements import *


class DataBase:
    def __init__(self, host, database, user, password):
        self.credentials = {'host': host,
                            'database': database,
                            'user': user,
                            'password': password}

    def connect(self):
        self.cnx = connector.connect(**self.credentials,
                                     buffered=True,
                                     pool_name='many')

    def close(self): self.cnx.close()

    def execute_many(self, query, params=[]):
        with self.cnx.cursor() as cur:
            cur.executemany(query, params)
            self.cnx.commit()

            return cur

    def execute(self, query, params=[]):
        with connector.connect(**self.credentials,
                               buffered=True,
                               raise_on_warnings=True,
                               pool_name='one') as cnx:
            with cnx.cursor() as cur:
                cur.execute(query, params)
                cnx.commit()

                return cur


class Operations:
    def __init__(self, database, downdir):
        self.database = database
        self.downdir = downdir
        self.db_name = self.database.credentials['database']
        self.columns = self.get_all_columns()
        self.loaded = self.get_loaded()

    def get_all_columns(self):
        '''
        Ritorna un indice di tutte le colonne per ogni tabella del db.
        '''
        results = ((tab, col)
                   for tab, col in self.database.execute(GET_ALL_COLUMNS))

        columns = defaultdict(list)
        for tab, col in results:
            columns[tab].append(col)

        return columns

    def get_loaded(self):
        '''
        Ritorna l'elenco dei file già caricati nel db.
        '''
        return tuple(
            (file[0] for file in self.database.execute(GET_LOADED)))

    def reader(self, filepath, refcols):
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
    def batched_rows(reader, n):
        '''
        Il generatore crea pacchetti di linee da inserire nel db usando 
        il metodo executemany() previsto dal connettore mysql
        '''
        while (batch := tuple(islice(reader, n))):
            yield batch

    def creator(self,
                stmts,
                params=[],
                tables=[],
                dirs=[],
                files=[],
                hash=False,
                key=True):
        '''
        Crea le tabelle qualora non siano già presenti nel db. Eventualmente 
        aggiunge "id" primary key ed "hash" unique key.
        '''
        if tables or dirs or files:
            if tables:
                tables = (t.replace('_', '-') for t in tables)

            elif dirs:
                tables = (d.split(os.sep)[-1] for d in dirs)

            elif files:
                tables = (f.split(os.sep)[-2] for f in files)

        else:
            tables = (tab for tab in stmts)

        for tab in tables:
            try:
                self.database.execute(stmts[tab], params)

                tab = tab.replace('-', '_')
                columns = self.get_all_columns()[tab]

                if hash:
                    columns = ','.join(columns)
                    hash_stmt = HASH_KEY.format(tab, tab, columns)
                    self.database.execute(hash_stmt)

                if key:
                    pk_stmt = ADD_ID.format(tab, tab)
                    self.database.execute(pk_stmt)

            except errors.Error as e:
                if e.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    logging.warning(f'{tab}: {e}')

                else:
                    logging.error(f'{tab}: {e}')

            else:
                logging.info(f'CREATED : "{tab}"')

            finally:
                self.columns = self.get_all_columns()

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

        insert_stmt = INSERT_TABLES.format(
            table_name, columns, values)

        return insert_stmt

    def insert(self,
               file,
               table,
               batch_size):
        '''
        Esegue l'insert nel db. 
        '''
        insert_stmt = self.format_insert(file, table)

        n_rows = 0

        refcols = self.columns[table]
        row = self.reader(file, refcols)
        batches = self.batched_rows(row, batch_size)
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

        last_ins, = self.database.execute(LAST_LOAD).fetchone()
        last_ins = last_ins or datetime(1990, 1, 1)

        logging.info(f'INSERT : "sintesi"')

        insert_sintesi = INSERT_SINTESI.format(columns)
        params = (last_ins, last_ins, last_ins, RGX_DENOMINAZIONE)

        try:
            self.database.connect()
            rows = self.database.execute_many(
                insert_sintesi, [params]).rowcount

        except errors.Error as e:
            logging.error(f'{e}')

        else:
            logging.info(f'INSERT : {rows} rows inserted into sintesi')

            return rows

        finally:
            self.database.close()

    def loader(self,
               dirs=[],
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

        if dirs:
            for d in dirs:
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
                    if not dirs or dir.path in dirs:
                        for file in os.scandir(dir):
                            n_rows = 0
                            if not files or file.path in files:
                                if file.name not in self.loaded:
                                    logging.info(
                                        f'INSERT : "{file.name}" into "{tab.name}"')

                                    try:
                                        n_rows = self.insert(
                                            file.path, tab.name, BATCH_SIZE)

                                    except errors.Error as e:
                                        logging.error(f'{e}')

                                    else:
                                        tot_rows += n_rows

                                        self.database.execute_many(
                                            INSERT_LOADED, [(tab.name, file.name)])

                                        if clean and tab.name not in ('cpv', 'province'):
                                            try:
                                                os.remove(file.path)
                                                logging.info(
                                                    f'"{file.path}" deleted')

                                            except FileNotFoundError:
                                                ...

                                    finally:
                                        self.database.close()

                                else:
                                    logging.warning(
                                        f'"{file.name}" already loaded')

            if tot_rows > 0:
                logging.info(
                    f'INSERT : {tot_rows} row inserted into "{tab.name}"')

        logging.info('INSERT : COMPLETED')
