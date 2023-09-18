
import json
import logging
from datetime import datetime, MINYEAR
from itertools import islice

from mysql import connector
from mysql.connector import errorcode, errors

from anac import statements as stmts


class DataBase:
    def __init__(self, host, database, user, password):
        self.credentials = {
            'host': host,
            'database': database,
            'user': user,
            'password': password}

    def execute_many(self, stmt, params=()):
        with connector.connect(**self.credentials,
                               buffered=True,
                               pool_name='many',
                               autocommit=True) as cnx:
            with cnx.cursor() as cur:
                cur.executemany(stmt, params)

                return cur

    def execute(self, query, params=()):
        with connector.connect(**self.credentials,
                               buffered=True,
                               raise_on_warnings=True,
                               pool_name='one',
                               autocommit=True) as cnx:
            with cnx.cursor(named_tuple=True) as cur:
                cur.execute(query, params)

                return cur


class Operations:
    def __init__(self, database):
        self.database = database
        self.db_name = self.database.credentials['database']
        self.loaded = self.get_loaded()
        self.columns = None

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, value):
        self._columns = value

    def get_loaded(self):
        '''
        Ritorna l'elenco dei file già caricati nel db.
        '''
        loaded = ()
        try:
            loaded = tuple(
                row.file_name for row in self.database.execute(stmts.GET_LOADED))

        except errors.ProgrammingError as err:
            if err.errno == errorcode.ER_NO_SUCH_TABLE:
                self.database.execute(stmts.CREATE_LOADED)

                logging.info('CREATE : "loaded"')

            else:
                logging.exception(err)

        return loaded

    def get_columns(self, table):
        '''
        Ritorna le tabelle contenute in una tabella.
        '''
        return tuple(row.COLUMN_NAME for row in self.database.execute(
            stmts.GET_TABLE_COLUMNS, (table,)))

    @staticmethod
    def reader(file, refcols):
        '''
        Generatore che ritorna una riga dai file json selezionando solo
        le colonne presenti anche nel database (a volte nei files vengono
        aggiunte delle nuove colonne non presenti nel db o con nomi differenti).
        Inoltre assicura che i campi vuoti siano avvalorati correttamente come
        "None" in modo che il connettore li traduca in NULL durante l'inserimento.
        '''
        def fix(row):
            select = {}
            for k in sorted(row, key=len):
                _k = k.replace('-', '_')
                if refcols:
                    for col in sorted(refcols, key=len, reverse=True):
                        starts = _k.lower().startswith(col.lower())
                        if starts and col not in select:
                            select[col] = row[k] or None
                            break
                else:
                    select[_k] = row[k] or None

            return select

        yield from (json.loads(row, object_hook=fix) for row in file)

    @staticmethod
    def batched_rows(reader, batch_size):
        '''
        Il generatore crea pacchetti di righe da inserire nel db usando
        il metodo executemany() previsto dal connettore MySQL.
        '''
        while (batch := tuple(islice(reader, batch_size))):
            yield batch

    def create(self, statements, table,
               hash=False, key=True):
        '''
        Crea le tabelle qualora non siano già presenti nel db. Eventualmente
        aggiunge "id" primary key ed "hash" unique key.
        '''
        try:
            self.database.execute(statements[table])

        except errors.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                self.columns = self.get_columns(table)
            else:
                logging.exception(err)

        else:
            self.columns = self.get_columns(table)

            if hash:
                columns = ','.join(self.columns)
                hash_stmt = stmts.HASH_KEY.format(table, table, columns)
                self.database.execute(hash_stmt)

            if key:
                pk_stmt = stmts.ADD_ID.format(table, table)
                self.database.execute(pk_stmt)

            logging.info('CREATE : "%s" created', table)

    def insert(self, table_name, data):
        '''
        Esegue l'insert nel db.
        '''
        columns = ','.join(self.columns)

        values = [f'%({c})s' for c in self.columns]
        values = ','.join(values)

        insert_stmt = stmts.INSERT_TABLES.format(
            table_name, columns, values)

        rows = self.database.execute_many(insert_stmt, data).rowcount

        return rows

    def insert_sintesi(self):
        '''
        Inserisce i dati nella tabella "sintesi" verificando che non siano già
        stati inseriti in precedenza confrontando la data di inserimento.
        '''
        last, = self.database.execute(stmts.LAST_LOAD).fetchone()
        last = last or datetime(MINYEAR, 1, 1)

        logging.info('INSERT : "sintesi" ...')

        # params = (last, last, last, stmts.RGX_DENOMINAZIONE)
        params = (stmts.RGX_DENOMINAZIONE, last)
        rows = self.database.execute_many(
            stmts.INSERT_SINTESI, (params,)).rowcount

        logging.info('INSERT : %s rows into sintesi', rows)

        return rows

    def load(self, file, tab_name, file_name):
        '''
        Gestisce l'inserimento dei file ed aggiorna la tabella "loaded".
        '''
        nrows = 0

        logging.info(
            'INSERT : "%s" into "%s" ...', file_name, tab_name)

        reader = self.reader(file, self.columns)
        batches = self.batched_rows(reader, stmts.BATCH_SIZE)

        for batch in batches:
            nrows += self.insert(tab_name, batch)

        self.database.execute(stmts.INSERT_LOADED, (tab_name, file_name))

        return nrows
