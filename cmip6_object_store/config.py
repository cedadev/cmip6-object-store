import os
from configparser import SafeConfigParser

CONFIG = None
CONF_FILE = os.path.join(os.path.dirname(__file__), 'etc', 'config.ini')


def _parse_config(conf_file=CONF_FILE):

    parser = SafeConfigParser({'home': os.environ['HOME']})
    parser.read(conf_file)

    conf = {}

    for section in parser.sections():
        conf.setdefault(section, {})

        for key in parser.options(section):

            conf[section][key] = parser.get(section, key)

    return conf


def get_config():
    global CONFIG

    if not CONFIG:
        CONFIG = _parse_config()


get_config()
