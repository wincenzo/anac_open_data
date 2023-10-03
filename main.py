import argparse
import logging
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


def download_and_load(ops, tables):
    '''
    Esegue il download dei files in memoria, la creazione delle
    tabelle e l'inserimento dei file nelle tabelle, a meno che
    non siano stati inseriti in precedenza.
    '''
    with RemoteCKAN(stmts.URL_ANAC) as ckan:
        packages = ckan.action.package_list()

        for table, pack in index(packages):
            tot_rows = 0

            if table not in tables:
                continue

            results = ckan.action.package_show(id=pack)

            for res in results['resources']:

                format = res['format'] == 'JSON'
                mimetype = res['mimetype'] == 'application/zip'
                if not (format and mimetype):
                    continue

                url, name = res['url'], f'{res["name"]}.json'

                if name in ops.loaded:
                    logging.warning('"%s" already loaded', name)
                    continue

                ops.create(stmts.CREATE_TABLES, table, hash=True)

                try:
                    with urlopen(url) as res:
                        logging.info('DOWNLOAD : "%s" ...', name)

                        with (ZipFile(BytesIO(res.read())) as zfile,
                                zfile.open(name) as file):
                            reader = ops.get_rows(file, ops.columns)
                            rows = ops.load(reader, table, file.name)

                    tot_rows += rows

                except StopIteration:
                    continue

            if tot_rows:
                logging.info(
                    'INSERT : *** %s row inserted into "%s" ***', tot_rows, table)


def insert_user_tables(ops, tables, user_tabs=stmts.USER_TABS):
    '''
    Aggiunge le tabelle "cpv" e "province" non disponibili sul
    portale ANAC.
    '''
    for tab, path in user_tabs:
        if tab not in tables:
            continue

        with open(path) as file:
            if file.name in ops.loaded:
                logging.warning('"%s" already loaded', file.name)
                continue

            ops.create(stmts.CREATE_USER_TABLES, tab, hash=True)

            reader = ops.get_rows(file, ops.columns)
            rows = ops.load(reader, tab, file.name)

        logging.info(
            'INSERT : *** %s row inserted into "%s" ***', rows, tab)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='main')

    subparsers = parser.add_subparsers(
        title='subcommands', dest='command', required=True)

    dw_ld = subparsers.add_parser(
        'load', prog='make_db',
        description='executes all steps for db creation: download files-create tables-insert data')

    dw_ld.add_argument(
        '-t', '--tables', nargs='*', default=[], type=str, metavar='NAME',
        help='provide tables name to insert into db')

    dw_ld.add_argument(
        '-s', '--skip', nargs='*', default=['smartcig'], type=str, metavar='NAME',
        help='provide tables name to avoid, default value: "smartcig". If called without values no tables are skipped')

    sintesi = subparsers.add_parser(
        'sintesi', prog='make_sintesi',
        description='executes all steps to make the table "sintesi" and create the view "sintesi_cpv"')

    args = parser.parse_args()

    def main(args):
        if args.command == 'load':
            schema = stmts.CREATE_TABLES | stmts.CREATE_USER_TABLES

            for tab in args.tables:
                if tab not in schema:
                    raise ValueError(f'table "{tab}" not in database schema')

            tables = set(args.tables or schema) - set(args.skip)

            download_and_load(anac_ops, tables)
            insert_user_tables(anac_ops, tables)

        logging.info('*** COMPLETED ***')

    main(args)
