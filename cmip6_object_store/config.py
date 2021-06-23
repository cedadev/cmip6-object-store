import os
from configparser import ConfigParser
from itertools import chain

_thisdir = os.path.dirname(__file__)
_base = os.path.realpath(os.path.join(_thisdir, ".."))

CONFIG = None
CONF_FILE = os.path.join(_thisdir, "etc", "config.ini")


def _to_list(i):
    return i.split()


def _to_dict(i):
    if not i.strip():
        return {}
    return dict([_.split(":") for _ in i.strip().split("\n")])


def _to_bool(i):
    val = i.strip().lower()
    if val in ('true', 'yes', '1'):
        return True
    elif val in ('false', 'no', '0'):
        return False
    else:
        raise ValueError(f'{i} cannot be converted to boolean')


def _to_int(i):
    return int(i)


def _to_float(i):
    return float(i)


def _chain_config_types(conf, keys):
    return chain(*[conf.get("config_data_types", key).split() for key in keys])


def _get_mappers(conf):
    mappers = {}

    for key in _chain_config_types(conf, ["lists", "extra_lists"]):
        mappers[key] = _to_list

    for key in _chain_config_types(conf, ["dicts", "extra_dicts"]):
        mappers[key] = _to_dict

    for key in _chain_config_types(conf, ["ints", "extra_ints"]):
        mappers[key] = _to_int

    for key in _chain_config_types(conf, ["floats", "extra_floats"]):
        mappers[key] = _to_float

    for key in _chain_config_types(conf, ["bools", "extra_bools"]):
        mappers[key] = _to_bool

    return mappers


def _load_config(conf_file=CONF_FILE):

    conf = ConfigParser({"home": os.environ["HOME"],
                         "base_dir": _base})

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


def get_from_proj_or_workflow(key, project):

    proj_config = CONFIG[f"project:{project}"]
    if key in proj_config:
        return proj_config[key]
    return CONFIG["workflow"][key]
    
