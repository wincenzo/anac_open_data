import logging
from datetime import datetime
import os

now = datetime.now().strftime('%Y%m%dT%H%M%S')
os.makedirs('logs', exist_ok=True)
path = f'logs/ANAC_{now}.log'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s : %(levelname)s : %(funcName)s : %(message)s',
                    filename=path,
                    filemode='w')


console = logging.StreamHandler()
console.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(funcName)s : %(message)s')
console.setFormatter(formatter)

logging.getLogger().addHandler(console)
