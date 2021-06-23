import os

from abcunit_backend.database_handler import DataBaseHandler

from .. import logging
from ..config import CONFIG, get_from_proj_or_workflow

LOGGER = logging.getLogger(__file__)


def _setup_env(project):

    abcunit_db_settings_file = get_from_proj_or_workflow('abcunit_db_settings_file', project)

    with open(abcunit_db_settings_file) as f:
        value = f.read().strip()

    varname = 'ABCUNIT_DB_SETTINGS'
    if varname in os.environ and os.environ[varname] != value:
        LOGGER.warn(f'Changing value of ABCUNIT_DB_SETTINGS')

    LOGGER.debug(f'{varname} => {value} '
                 '(warning - may contain secrets - comment out this line in production')
    
    os.environ[varname] = value


    
def get_results_store(project):

    _setup_env(project)
    return DataBaseHandler(table_name=f'{project}_zarr_records')


def get_verification_store(project):

    _setup_env(project)
    return DataBaseHandler(table_name=f'{project}_zarr_verify_records')
