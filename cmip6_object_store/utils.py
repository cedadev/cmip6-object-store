import os
import json

from config import CONFIG


def get_credentials(creds_file=None):

    if not creds_file:
        creds_file = CONFIG['store']['credentials_file']

    with open(creds_file) as f:
        creds = json.load(f)

    return creds

