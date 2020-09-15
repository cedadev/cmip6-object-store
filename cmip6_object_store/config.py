import os
from configparser import SafeConfigParser

CONFIG = None
CONF_FILE = os.path.join(os.path.dirname(__file__), 'etc', 'config.ini')


def _parse_config(conf_file=CONF_FILE):

    parser = SafeConfigParser({'home': os.environ['HOME']})
    parser.read(conf_file)

    print(parser.options('datasets'))
    print(parser.get('datasets', 'datasets_file'))

    return None


def get_config():
    global CONFIG

    if not CONFIG:
        CONFIG = _parse_config()

    return CONFIG


get_config()
