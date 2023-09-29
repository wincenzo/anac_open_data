import argparse
import logging
import os
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

from ckanapi import RemoteCKAN

from anac import statements as stmts
from anac.load import DataBase, Operations

cnx = DataBase(**stmts.DB_CREDENTIALS)

anac_ops = Operations(database=cnx)


def index(pckgs):
    '''
    Crea un indice dei packages col nome della tabella associata.
    '''
    for pack in sorted(pckgs):
        for tab in sorted(stmts.CREATE_TABLES, reverse=True, key=len):
            if pack.startswith((tab.replace('_', '-'), tab)):
                yield tab, pack
                break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='main')

    subparsers = parser.add_subparsers(
        title='subcommands',
        dest='command',
        required=True)

    dw_ld = subparsers.add_parser(
        'load',
        prog='make_db',
        help='executes all steps for db creation: download files-create tables-insert data')

    dw_ld.add_argument(
        '-t', '--tables',
        nargs='*',
        default=[],
        type=str,
        metavar='NAME',
        help='provide tables name to insert into db; if missing insert all tables')

    dw_ld.add_argument(
        '-s', '--skip',
        nargs='*',
        default=['smartcig'],
        type=str,
        metavar='NAME',
        help='provide tables name to avoid download and load')

    sintesi = subparsers.add_parser(
        'sintesi',
        prog='make_sintesi',
        help='executes all steps to make the table "sintesi" and create the view "sintesi_cpv"')

    args = parser.parse_args()

    if args.command == 'load':
        def down_load(ops, skip=args.skip, tables=args.tables):
            '''
            Esegue il download dei files in memoria, la creazione delle
            tabelle e l'inserimento dei file nelle tabelle controllando
            che non siano stati inseriti in precedenza.
            '''
            with RemoteCKAN(stmts.URL_ANAC) as ckan:
                packages = ckan.action.package_list()

                tables = set(tables or stmts.CREATE_TABLES) - set(skip)

                for table, pack in index(packages):
                    tot_rows = 0

                    if table not in tables:
                        continue

                    results = ckan.action.package_show(id=pack)

                    for res in results['resources']:
                        format = res['format'] == 'JSON'
                        mimetype = res['mimetype'] == 'application/zip'

                        if format and mimetype:
                            url, name = res['url'], res['name']
                            file_name = f'{name}.json'

                            if file_name in ops.loaded:
                                logging.warning(
                                    '"%s" already loaded', file_name)
                                continue

                            ops.create(stmts.CREATE_TABLES, table, hash=True)

                            try:
                                with urlopen(url) as res:
                                    logging.info(
                                        'DOWNLOAD : "%s"', file_name)

                                    with ZipFile(BytesIO(res.read())) as zfile:
                                        with zfile.open(file_name) as file:
                                            rows = ops.load(
                                                file, table, file_name)

                                tot_rows += rows

                            except StopIteration:
                                continue

                    if tot_rows:
                        logging.info(
                            'INSERT : *** %s row inserted into "%s" ***', tot_rows, table)

        def insert_user_tables(ops, user_tabs=stmts.USER_TABS):
            '''
            Aggiunge le tabelle "cpv" e "province" non disponibili sul
            portale ANAC.
            '''
            for tab, path in user_tabs:
                if args.tables and tab not in args.tables:
                    continue

                file_name = os.path.basename(path)

                if file_name in ops.loaded:
                    logging.warning(
                        '"%s" already loaded', file_name)
                    continue

                ops.create(stmts.CREATE_TABLES, tab, hash=True)

                with open(path) as file:
                    rows = ops.load(file, tab, file_name)

                logging.info(
                    'INSERT : *** %s row inserted into "%s" ***', rows, tab)

        def make_db(ops):
            down_load(ops)
            insert_user_tables(ops)

            logging.info('*** COMPLETED ***')

        for tab in args.tables:
            assert tab in stmts.CREATE_TABLES, f'table "{tab}" not in database schema'

        make_db(anac_ops)
