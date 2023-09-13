import argparse
from collections import defaultdict
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile
import logging
import os
from pprint import pprint

from ckanapi import RemoteCKAN

from anac import load
from anac import statements as stmts


cnx = load.DataBase(**stmts.DB_CREDENTIALS)
anac_ops = load.Operations(
    database=cnx, downdir=stmts.DEFAULT_DOWNLOAD_PATH)


def index(packs):
    '''
    Crea un indice dei packages in base al nome della tabella
    '''
    idx = defaultdict(list)
    for pack in sorted(packs, key=len):
        for tab in sorted(stmts.TABLES, reverse=True, key=len):
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

    dw_ld.add_argument('-c', '--clean',
                       action='store_true',
                       help='deletes file after it is inserted into db')

    dw_ld.add_argument('-k', '--keep',
                       nargs='*',
                       default=[],
                       type=str,
                       metavar='NAME',
                       help='''provide tables name to keep when
                       "clean" option is called''')

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

    check = subparsers.add_parser('check',
                                  prog='check_columns',
                                  help='checks for columns mismatch between files and tables')

    args = parser.parse_args()

    if args.command == 'load':
        def down_n_load(ops,
                        skip=args.skip,
                        tables=args.tables,
                        clean=args.clean,
                        keep=args.keep):
            '''
            Esegue il download dei files, la creazione delle tabelle e 
            l'inserimento dei file nelle tabelle controllando che non 
            siano stati inseriti in precedenza
            '''
            if tables:
                for tab in tables:
                    assert tab in stmts.TABLES,\
                        f'table "{tab}" not in database schema'

            with RemoteCKAN(stmts.URL_ANAC) as api:
                packages = api.action.package_list()
                pckgs_idx = index(packages)

                tables = tables or pckgs_idx
                tables = set(tables) - set(skip)

                for table in tables:
                    tot_rows = 0
                    for pack in pckgs_idx[table]:
                        pack_path = os.path.join(
                            stmts.DEFAULT_DOWNLOAD_PATH, table, pack)

                        results = api.action.package_show(id=pack)

                        for file in results['resources']:
                            if (file['format'] == 'JSON' and
                                    file['mimetype'] == 'application/zip'):
                                url, name = file['url'], file['name']

                                file_name = f'{name}.json'
                                file_path = os.path.join(pack_path, file_name)

                                if file_name not in ops.loaded:
                                    ops.create(stmts.CREATE_TABLES,
                                               table, hash=True)

                                    if not os.path.isfile(file_path):
                                        logging.info(
                                            'DOWNLOAD : "%s"', file_path)

                                        with urlopen(url) as resp:
                                            zfile = BytesIO(resp.read())
                                            with ZipFile(zfile) as zfile:
                                                zfile.extractall(pack_path)
                                    else:
                                        logging.warning(
                                            '"%s" already donwloaded', file_path)

                                    ops.load(table, file_name, file_path)

                                    if clean and table not in keep:
                                        try:
                                            os.remove(file_path)
                                            logging.info(
                                                '"%s" deleted', file_path)

                                        except FileNotFoundError:
                                            ...

                                else:
                                    logging.warning(
                                        '"%s" already loaded', file_path)

                    if tot_rows:
                        logging.info(
                            'INSERT : %s row inserted into "%s"', tot_rows, table)

        def user_tables(ops, tables=args.tables):
            '''
            Aggiunge le tabelle "cpv" e "province" non disponibili sul 
            portale ANAC
            '''
            tabs = (('cpv', 'cpv_tree.json'),
                    ('province', 'province.json'))

            for tab, path in tabs:
                if not tables or tab in tables:
                    ops.create(stmts.CREATE_TABLES, tab, hash=True)

                    tot_rows = 0
                    file_name = os.path.basename(path)
                    if file_name not in ops.loaded:
                        nrows = ops.load(tab, file_name, path)
                        tot_rows += nrows

                    else:
                        logging.warning(
                            '"%s" already loaded', path)

                    if tot_rows:
                        logging.info(
                            'INSERT : %s row inserted into "%s"', tot_rows, tab)

        down_n_load(anac_ops)

        user_tables(anac_ops)

    elif args.command == 'sintesi':
        def make_sintesi(ops):
            '''
            Esegue tutte le operazioni necessarie a creare ed inserire i dati nella
            tabella "sintesi"
            '''
            ops.create(stmts.CREATE_SINTESI, 'sintesi', hash=True)
            ops.insert_sintesi()

            ops.create(stmts.CREATEVIEW_SINTESI_CPV,
                       'sintesi_cpv', key=False, hash=False)

        make_sintesi(anac_ops)

    elif args.command == 'check':
        def check_columns(ops):
            '''
            Trova eventuali colonne presenti nei file,
            ma assenti nelle tabelle del db
            '''
            missing = defaultdict(list)
            for d in os.scandir(ops.downdir):
                sub = next(os.scandir(d))
                for f in os.scandir(sub):
                    if os.stat(f).st_size > 0:
                        row = next(ops.reader(f.path, refcols=None))
                        row = sorted(row, key=len)

                        db_cols = list(ops.get_columns(dir.name))
                        for col in row:
                            for c in sorted(db_cols, reverse=True, key=len):
                                if col.lower().startswith(c.lower()):
                                    db_cols.remove(c)
                                    break
                            else:
                                missing[dir.name].append(col)
                        break
            return missing

        diff = check_columns(anac_ops)
        pprint(diff)
