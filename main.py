import argparse
from collections import defaultdict
import logging
import os
from pprint import pprint

import ANAC.download as download
import ANAC.load as load
import ANAC.statements as stmts


anac_db = load.DataBase(**stmts.DB_CREDENTIALS)

anac_ops = load.Operations(database=anac_db,
                           downdir=stmts.DEFAULT_DOWNLOAD_PATH)


# def check_columns(ops):
#    '''
#    Serve a trovare eventuali colonne presenti nei file,
#    ma assenti nelle tabelle del db
#    '''
#    missing = defaultdict(list)
#
#    for dir in os.scandir(ops.downdir):
#        sub = next(os.scandir(dir))
#        for f in os.scandir(sub):
#            if os.stat(f).st_size > 0:
#                row = next(ops.gen_row(f.path, None))
#                row = sorted(row, key=len)
#
#                db_cols = ops.get_all_columns()[dir.name]
#                for col in row:
#                    for c in sorted(db_cols, reverse=True, key=len):
#                        if col.lower().startswith(c.lower()):
#                            db_cols.remove(c)
#                            break
#                    else:
#                        missing[dir.name].append(col)
#                break
#
#    return missing


def make_database(ops, tables=[], directories=[], files=[], clean=False,):
    '''
    esegue tutte le operazioni necessarie a creare il database:
    scaricare i file, creare viste e tabelle se non già esistenti,
    inserire i dati dai file nelle rispettive tabelle, eventualmente
    cancellare i file già inseriti.
    '''
    if not directories and not files:
        download.download(anac_db, tables=tables)

    ops.creator(stmts.CREATE_TABLES, key=True, hash=True)
    ops.creator(stmts.CREATE_LOADED, key=True, hash=False)
    ops.creator(stmts.CREATE_SINTESI, key=True, hash=True)
    ops.creator(stmts.CREATEVIEW_SINTESI_CPV, key=False, hash=False)

    ops.loader(directories=directories,
               files=files,
               clean=clean,
               tables=tables)

    ops.insert_sintesi()

    logging.info(f'OPERATIONS: COMPLETED')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='main')

    subparsers = parser.add_subparsers(title='subcommands',
                                       dest='command',
                                       required=True,
                                       )

    loadirs = subparsers.add_parser('loadirs',
                                    prog='make_database',
                                    help='executes all steps for db creation: download files-create tables-insert data',
                                    )

    # check = subparsers.add_parser('check',
    #                              prog='check_columns',
    #                              help='checks for columns mismatch between files and tables',
    #                              )

    loadirs.add_argument('-c', '--clean',
                         action='store_true',
                         help='deletes files from download directory after inserting',
                         )

    loadirs.add_argument('-d', '--dirs',
                         nargs='*',
                         default='',
                         type=str,
                         metavar='PATH',
                         help='provide directories path to insert into db; if missing insert whole download directory',
                         )

    loadirs.add_argument('-f', '--files',
                         nargs='*',
                         default='',
                         type=str,
                         metavar='PATH',
                         help='provide files path to insert into db; if missing insert whole download directory',
                         )

    loadirs.add_argument('-t', '--tables',
                         nargs='*',
                         default='',
                         type=str,
                         metavar='NAME',
                         help='provide tables name to insert into db; if missing insert all tables',
                         )

    args = parser.parse_args()

    if args.command == 'loadirs':
        make_database(anac_ops,
                      directories=args.dirs,
                      files=args.files,
                      clean=args.clean,
                      tables=args.tables)

    # elif args.command == 'check':
    #    diff = check_columns(anac_ops)
    #    pprint(diff)
