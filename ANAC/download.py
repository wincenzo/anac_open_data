from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile
import logging
import os

from ckanapi import RemoteCKAN

from .statements import (DEFAULT_DOWNLOAD_PATH,
                         GET_LOADED,
                         URL_ANAC,
                         TABLES)


def download(ops,
             download_dir=DEFAULT_DOWNLOAD_PATH,
             tables=[]):
    '''
    Esegue il download dei file ed organizza la directory di download
    raggruppando i file in base alla tabella di appartenenza
    '''
    with RemoteCKAN(URL_ANAC) as api:
        packages = api.action.package_list()

        for pack in sorted(packages, key=len):
            table = ''
            for tab in sorted(TABLES, reverse=True, key=len):
                if pack.startswith(tab):
                    table = tab.replace('-', '_')
                    break
            else:
                logging.warning(f'NEW: "{pack}" available')

            if not tables or table in tables:

                tab_path = os.path.join(download_dir, table, pack)

                files = api.action.package_show(id=pack)

                for file in files['resources']:
                    if (file['format'] == 'JSON' and
                            file['mimetype'] == 'application/zip'):
                        url, name = file['url'], file['name']

                        file_name = f'{name}.json'
                        file_path = os.path.join(tab_path, file_name)

                        if not os.path.isfile(file_path):
                            if file_name not in ops.loaded:
                                logging.info(f'DOWNLOAD : "{file_path}"')

                                with urlopen(url) as resp:
                                    zfile = BytesIO(resp.read())
                                    with ZipFile(zfile) as zfile:
                                        zfile.extractall(tab_path)

                            else:
                                logging.warning(
                                    f'"{file_path}" already loaded')
                        else:
                            logging.warning(
                                f'"{file_path}" already donwloaded')

        logging.info('DOWNLOAD : COMPLETED')
