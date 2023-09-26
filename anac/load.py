
import json
import logging
import sys
from itertools import islice

from mysql.connector import errorcode, errors, pooling

from anac import statements as stmts


class DataBase:
    def __init__(self, host, database, user, password):
        self.pool = pooling.MySQLConnectionPool(
            host=host,
            database=database,
            user=user,
            password=password,
            pool_name='anac',
            buffered=True,
            autocommit=True)

    def execute(self, stmt, params=(), many=False):
        with self.pool.get_connection() as cnx:
            if many:
                with cnx.cursor() as cur:
                    cur.executemany(stmt, params)

            else:
                with cnx.cursor(named_tuple=True) as cur:
                    cur.execute(stmt, params)

        return cur


class Operations:
    def __init__(self, database):
        self.database = database
        self.columns = None

        try:
            self.loaded = tuple(
                row.file_name for row in self.database.execute(
                    stmts.GET_LOADED))

        except errors.Error as err:
            if err.errno == errorcode.ER_NO_SUCH_TABLE:
                self.database.execute(stmts.CREATE_LOADED)

                self.loaded = ()

                logging.info('CREATE : "loaded"')

            else:
                logging.exception(err)
                sys.exit(1)

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, value):
        self._columns = value

    def get_columns(self, table):
        '''
        Ritorna le colonne contenute in una tabella.
        '''
        return tuple(row.COLUMN_NAME for row in self.database.execute(
            stmts.GET_TABLE_COLUMNS, (table,)))

    @staticmethod
    def batched_rows(file, refcols, batch_size):
        '''
        Il generatore crea pacchetti di righe da inserire nel db usando
        il metodo executemany() previsto dal connettore MySQL. Seleziona solo
        le colonne presenti anche nel database (a volte nei files vengono
        aggiunte delle nuove colonne non presenti nel db o con nomi differenti).
        Inoltre assicura che i campi vuoti siano avvalorati correttamente in
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

        reader = (json.loads(row, object_hook=fix) for row in file)

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
                sys.exit(1)

        else:
            self.columns = self.get_columns(table)

            if hash:
                columns = ','.join(self.columns)
                hash_stmt = stmts.HASH_KEY.format(table, table, columns)
                self.database.execute(hash_stmt)

            if key:
                pk_stmt = stmts.ADD_ID.format(table, table)
                self.database.execute(pk_stmt)

            logging.info('CREATE : "%s"', table)

    def insert(self, table_name, data):
        '''
        Esegue l'insert nel db.
        '''
        columns = ','.join(self.columns)

        values = ','.join(f'%({c})s' for c in self.columns)

        insert_stmt = stmts.INSERT_TABLES.format(
            table_name, columns, values)

        rows = self.database.execute(
            insert_stmt, data, many=True).rowcount

        return rows

    def load(self, file, tab_name, file_name):
        '''
        Gestisce l'inserimento dei file ed aggiorna la tabella "loaded".
        '''
        logging.info(
            'INSERT : "%s" into "%s" ...', file_name, tab_name)

        batches = self.batched_rows(file, self.columns, stmts.BATCH_SIZE)

        rows = 0
        for batch in batches:
            rows += self.insert(tab_name, batch)

        self.database.execute(stmts.INSERT_LOADED, (tab_name, file_name))

        return rows
