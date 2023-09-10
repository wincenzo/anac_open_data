
from itertools import islice
from datetime import datetime
import json
import logging
import os

import mysql.connector as connector
from mysql.connector import errors, errorcode

from .statements import *


class DataBase:
    def __init__(self, host, database, user, password):
        self.credentials = {'host': host,
                            'database': database,
                            'user': user,
                            'password': password}

    def execute_many(self, query, params=()):
        with connector.connect(**self.credentials,
                               buffered=True,
                               pool_name='many') as cnx:
            with cnx.cursor() as cur:
                cur.executemany(query, params)
                cnx.commit()

                return cur

    def execute(self, query, params=()):
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
        self.loaded = self.get_loaded()

    def get_loaded(self):
        '''
        Ritorna l'elenco dei file già caricati nel db.
        '''
        loaded = ()
        try:
            loaded = tuple(row[0]
                           for row in self.database.execute(GET_LOADED))

        except errors.ProgrammingError as e:
            if e.errno == errorcode.ER_NO_SUCH_TABLE:
                self.database.execute(CREATE_LOADED)

                logging.info('CREATE : "loaded"')

        return loaded

    def get_columns(self, table):
        '''
        Ritorna le tabelle contenute in una tabella
        '''
        return tuple(row[0] for row in self.database.execute(
            GET_TABLE_COLUMNS, (table,)))

    @staticmethod
    def reader(filepath, refcols):
        '''
        Generatore che ritorna una riga dai file json e selezionando solo 
        le colonne presenti anche nel database (a volte nei files vengono 
        aggiunte delle nuove colonne non presenti nel db o con nomi differenti). 
        Inoltre assicura che i campi vuoti siano avvalorati correttamente e 
        che i valori nulli siano istanziati come "None" in modo che il 
        connettore li traduca in NULL durante l'inserimento.
        '''
        def fix(row):
            select = {}
            for k in sorted(row, key=len):
                _k = k.replace('-', '_')
                if refcols:
                    for col in sorted(refcols, reverse=True, key=len):
                        if (_k.lower().startswith(col.lower())
                                and col not in select):
                            select[col] = row[k] or None
                            break
                else:
                    select[_k] = row[k] or None

            return select

        with open(filepath) as file:
            yield from (json.loads(row, object_hook=fix) for row in file)

    @staticmethod
    def batched_rows(reader, batch_size):
        '''
        Il generatore crea pacchetti di linee da inserire nel db usando 
        il metodo executemany() previsto dal connettore mysql
        '''
        while (batch := tuple(islice(reader, batch_size))):
            yield batch

    def create(self, stmts, table,
               hash=False, key=True):
        '''
        Crea le tabelle qualora non siano già presenti nel db. Eventualmente 
        aggiunge "id" primary key ed "hash" unique key.
        '''
        try:
            TABLE = table.replace('_', '-')
            self.database.execute(stmts[TABLE])

        except errors.DatabaseError as e:
            if e.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                self.columns = self.get_columns(table)

        else:
            self.columns = self.get_columns(table)

            if hash:
                columns = ','.join(self.columns)
                hash_stmt = HASH_KEY.format(table, table, columns)
                self.database.execute(hash_stmt)

            if key:
                pk_stmt = ADD_ID.format(table, table)
                self.database.execute(pk_stmt)

            logging.info(f'CREATE : "{table}" created')

    def format_insert(self, table_name):
        '''
        Nel caso il file non sia vuoto (sono presenti file vuoti), formatta 
        dinamicamente l'insert statement per ogni tabella.
        '''
        columns = ','.join(self.columns)
        values = [f'%({c})s' for c in self.columns]
        values = ','.join(values)

        insert_stmt = INSERT_TABLES.format(table_name, columns, values)

        return insert_stmt

    def insert(self, file_path, table_name, batch_size):
        '''
        Esegue l'insert nel db. 
        '''
        insert_stmt = self.format_insert(table_name)

        rows = 0
        reader = self.reader(file_path, self.columns)
        batches = self.batched_rows(reader, batch_size)
        for batch in batches:
            rows += self.database.execute_many(insert_stmt, batch).rowcount

        return rows

    def insert_sintesi(self):
        '''
        Inserisce i dati nella tabella "sintesi" verificando che non siano già 
        stati inseriti in precedenza confrontando la data di inserimento.
        '''
        columns = ','.join(self.columns)

        last_ins, = self.database.execute(LAST_LOAD).fetchone()
        last_ins = last_ins or datetime(1990, 1, 1)

        logging.info(f'INSERT : "sintesi"')

        insert_sintesi = INSERT_SINTESI.format(columns)
        params = (last_ins, last_ins, last_ins, RGX_DENOMINAZIONE)

        rows = self.database.execute_many(
            insert_sintesi, [params]).rowcount

        logging.info(f'INSERT : {rows} rows into sintesi')

        return rows

    def load(self, tab_name, file_name, file_path):
        '''
        Gestisce l'inserimento dei file ed aggiorna la tabella "loaded".
        '''
        nrows = 0

        if os.stat(file_path).st_size > 0:
            logging.info(
                f'INSERT : "{file_name}" into "{tab_name}"')

            nrows = self.insert(
                file_path, tab_name, BATCH_SIZE)

            self.database.execute_many(
                INSERT_LOADED, ((tab_name, file_name),))

        else:
            logging.warning(f'INSERT : {file_path} is empty')

        return nrows
