import os
from itertools import chain
from configparser import SafeConfigParser

CONFIG = None
CONF_FILE = os.path.join(os.path.dirname(__file__), 'etc', 'config.ini')


def OLD_parse_config(conf_file=CONF_FILE):

    parser = SafeConfigParser({'home': os.environ['HOME']})
    parser.read(conf_file)

    conf = {}

    for section in parser.sections():
        conf.setdefault(section, {})

        for key in parser.options(section):

            conf[section][key] = parser.get(section, key)

    return conf


def _to_list(i): return i.split()


def _to_dict(i):
    if not i.strip(): return {}
    return dict([_.split(':') for _ in i.strip().split('\n')])


def _to_int(i): return int(i)


def _to_float(i): return float(i)


def _chain_config_types(conf, keys):
    return chain(*[conf.get('config_data_types', key).split() for key in keys])


def _get_mappers(conf):
    mappers = {}

    for key in _chain_config_types(conf, ['lists', 'extra_lists']):
        mappers[key] = _to_list

    for key in _chain_config_types(conf, ['dicts', 'extra_dicts']):
        mappers[key] = _to_dict

    for key in _chain_config_types(conf, ['ints', 'extra_ints']):
        mappers[key] = _to_int

    for key in _chain_config_types(conf, ['floats', 'extra_floats']):
        mappers[key] = _to_float

    return mappers


def _load_config(conf_file=CONF_FILE):

    conf = SafeConfigParser({'home': os.environ['HOME']})

    conf.read(conf_file)
    config = {}

    mappers = _get_mappers(conf)

    for section in conf.sections():
        config.setdefault(section, {})

        for key in conf.options(section):

            value = conf.get(section, key)

            if key in mappers:
                value = mappers[key](value)

            config[section][key] = value

    return config

def get_config():
    global CONFIG

    if not CONFIG:
        CONFIG = _load_config()


get_config()
