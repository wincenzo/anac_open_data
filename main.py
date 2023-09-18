import argparse
import logging
import os
from collections import defaultdict
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

from ckanapi import RemoteCKAN

from anac.load import DataBase, Operations
from anac import statements as stmts


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

    subparsers = parser.add_subparsers(title='subcommands',
                                       dest='command',
                                       required=True)

    dw_ld = subparsers.add_parser('load',
                                  prog='make_database',
                                  help='''executes all steps for db creation:
                                  download files-create tables-insert data''')

    dw_ld.add_argument('-t', '--tables',
                       nargs='*',
                       default=[],
                       type=str,
                       metavar='NAME',
                       help='''provide tables name to insert into db;
                       if missing insert all tables''')

    dw_ld.add_argument('-s', '--skip',
                       nargs='*',
                       default=['smartcig'],
                       type=str,
                       metavar='NAME',
                       help='provide tables name to avoid download and load')

    sintesi = subparsers.add_parser('sintesi',
                                    prog='make_sintesi',
                                    help='''executes all steps to make the table
                                    "sintesi" and create the view "sintesi_cpv"''')

    args = parser.parse_args()

    if args.command == 'load':
        def down_n_load(ops,
                        skip=args.skip,
                        tables=args.tables):
            '''
            Esegue il download dei files, la creazione delle tabelle e
            l'inserimento dei file nelle tabelle controllando che non
            siano stati inseriti in precedenza.
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
                                            zfile = BytesIO(res.read())
                                            with ZipFile(zfile) as zfile:
                                                with zfile.open(file_name) as file:
                                                    nrows = ops.load(
                                                        file, table, file_name)
                                                    tot_rows += nrows

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
            Aggiunge le tabelle "cpv" e "province" non disponibili sul portale ANAC.
            '''
            tabs = (('cpv', 'cpv_tree.json'),
                    ('province', 'province.json'))

            for tab, path in tabs:
                if not tables or tab in tables:
                    ops.create(stmts.CREATE_TABLES, tab, hash=True)

                    file_name = os.path.basename(path)
                    if file_name not in ops.loaded:
                        nrows = ops.load(tab, file_name, path)
                        logging.info(
                            'INSERT : %s row inserted into "%s"', nrows, tab)

                    else:
                        logging.warning(
                            '"%s" already loaded', path)

        def make_db(ops):
            down_n_load(ops)
            user_tables(ops)

            logging.info('COMPLETED')

        make_db(anac_ops)

    elif args.command == 'sintesi':
        def make_sintesi(ops):
            '''
            Esegue tutte le operazioni necessarie a creare ed inserire i dati nella
            tabella "sintesi".
            '''
            ops.create(stmts.CREATE_SINTESI, 'sintesi', hash=True)
            ops.insert_sintesi()

            ops.create(stmts.CREATEVIEW_SINTESI_CPV,
                       'sintesi_cpv', key=False, hash=False)

        make_sintesi(anac_ops)
