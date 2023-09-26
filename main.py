import argparse
import logging
import os
from collections import defaultdict
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
    Crea un indice dei package in base al nome della tabella.
    '''
    idx = defaultdict(list)
    for pack in sorted(pckgs, key=len):
        for tab in sorted(stmts.CREATE_TABLES, reverse=True, key=len):
            if pack.startswith((tab.replace('_', '-'), tab)):
                idx[tab].append(pack)
                idx[tab].sort()
                break
    return idx


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

    args = parser.parse_args()

    if args.command == 'load':
        def down_n_load(ops, skip=args.skip, tables=args.tables):
            '''
            Esegue il download dei files in memoria, la creazione delle
            tabelle e l'inserimento dei file nelle tabelle controllando
            che non siano stati inseriti in precedenza.
            '''
            for tab in tables:
                assert tab in stmts.CREATE_TABLES,\
                    f'table "{tab}" not in database schema'

            with RemoteCKAN(stmts.URL_ANAC) as api:
                packages = api.action.package_list()
                pckgs_idx = index(packages)

                tables = set(tables or pckgs_idx) - set(skip)

                for table in tables:
                    tot_rows = 0

                    for pack in pckgs_idx[table]:
                        results = api.action.package_show(id=pack)

                        for res in results['resources']:
                            format = res['format'] == 'JSON'
                            mimetype = res['mimetype'] == 'application/zip'

                            if format and mimetype:
                                url, name = res['url'], res['name']
                                file_name = f'{name}.json'

                                if file_name not in ops.loaded:
                                    ops.create(stmts.CREATE_TABLES,
                                               table, hash=True)

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

                                else:
                                    logging.warning(
                                        '"%s" already loaded', file_name)

                    if tot_rows:
                        logging.info(
                            'INSERT : *** %s row inserted into "%s" ***', tot_rows, table)

        def user_tables(ops, tables=args.tables):
            '''
            Aggiunge le tabelle "cpv" e "province" non disponibili sul
            portale ANAC.
            '''
            tabs = (('cpv', 'cpv_tree.json'),
                    ('province', 'province.json'))

            for tab, path in tabs:
                if not tables or tab in tables:
                    ops.create(stmts.CREATE_TABLES, tab, hash=True)

                    file_name = os.path.basename(path)
                    if file_name not in ops.loaded:
                        with open(path) as file:
                            rows = ops.load(file, tab, file_name)

                            logging.info(
                                'INSERT : *** %s row inserted into "%s" ***', rows, tab)

                    else:
                        logging.warning(
                            '"%s" already loaded', file_name)

        def make_db(ops):
            down_n_load(ops)
            user_tables(ops)

            logging.info('*** COMPLETED ***')

        make_db(anac_ops)
