#!/usr/bin/env python

"""
CURRENT USAGE:
This file looks up the live CMIP6 CREPP database to find data that is current in the CEDA CMIP6 archive

Must be run on esgf-pub to have access to live CREPP database.
Must be Python2.7 do not update to Python3 at this time

TODO: update to import req_vars from data dir

cd /usr/local/crepp/
source setup_env.sh

Run:
python $PATH_TO_THIS_FILE/generate_xarray_zarr_play_datasets.py <output-directory>

Although output-directory is optional argument is strongly recommended
"""

import os
import sys
import django
from datetime import datetime as dt
from django.db.models import Q
from django.db.models import Sum
from functools import reduce
from operator import or_
from crepp_app.models import *
from crepplib.vocabs import *
from crepp_site import settings_local
from crepplib.vocabs import PROCESSING_STATUS_VALUES, STATUS_VALUES, ACTION_TYPES, CHECKSUM_TYPES
print('using database {}'.format(settings_local.DATABASES['default']['NAME']))
django.setup()

try:
    odir = sys.argv[1]
except IndexError:
    odir = os.getcwd()

TODAYS_DATE = dt.today().isoformat().split('T')[0]
ofile = odir + 'cmip6-datasets_{}.csv'.format(TODAYS_DATE)
print("Output file: ", ofile)


def get_dataset_size(ds):
    files = ds.file_set.all()
    return files.count(), round((files.aggregate(Sum('size'))['size__sum'])/(1024.*1024), 2)

req_vars = ["Amon.clt", "Amon.evspsbl", "Amon.hfls", "Amon.hfss", "Amon.hurs", "Amon.huss", "Amon.pr",
              "Amon.prsn", "Amon.ps", "Amon.psl", "Amon.rlds", "Amon.rlus", "Amon.rlut", "Amon.rsds",
              "Amon.rsdt", "Amon.rsus", "Amon.rsut", "Amon.sfcWind", "Amon.tas", "Amon.tasmax", "Amon.tasmin",
              "Amon.tauu", "Amon.tauv", "Amon.ts", "Amon.uas", "Amon.vas", "Amon.zg", "LImon.snw", "Lmon.mrro",
              "Lmon.mrsos", "OImon.siconc", "OImon.sim", "OImon.sithick", "OImon.snd", "OImon.tsice", "Omon.sos",
              "Omon.tos", "Omon.zos", "Amon.ta", "Amon.ua", "Amon.va", "Amon.hur", "Amon.hus", "Amon.zg",
              'Oday.tos', 'day.hurs', 'day.huss', 'day.mrro', 'day.pr', 'day.psl', 'day.sfcWindmax', 'day.snw',
              'day.tas', 'day.tasmax', 'day.tasmin','day.uas', 'day.vas', 'day.zg', 'CFday.ps', ]
              #  'day.ua', 'day.va', ]
              # '3hr.huss', '3hr.pr', '3hr.tas', '3hr.vas', '3hr.uas', '6hrPlev.zg1000']



valid_dss = {'is_withdrawn': False, 'is_paused': False, 'processing_status': PROCESSING_STATUS_VALUES.COMPLETED}

c3s34g_valid_ds = Dataset.objects.filter(**valid_dss)
valid_vars_filter = reduce(or_, [Q(name__icontains=val) for val in req_vars])
valid_datasets = c3s34g_valid_ds.filter(valid_vars_filter)

with open(ofile, 'a+') as w:
    w.writelines('Dataset_id, num_files, size (MB)\n')

    for ds in valid_datasets:
        nfiles, total_size = get_dataset_size(ds)
        w.writelines('{}, {}, {}\n'.format(ds.name, nfiles, total_size))

