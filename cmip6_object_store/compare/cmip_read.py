"CMIP6 data read comparison code"
import sys
import json
from pprint import pprint
import logging
import time
import os
import pandas as pd

import argparse
from random import randint


parser = argparse.ArgumentParser(description='Gather variables from command line', formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument(
    'config',
    help = "Config file location",

    )

parser.add_argument(
    '-l', '--log-level',
    action = 'store',
    dest = 'log_level',
    help = 'Set the log level of console output (CRITICAL, ERROR, WARNING, INFO, DEBUG). Default: INFO',
    required = False,
    default = 'INFO',
    )


class ConfigError(Exception):
    "Raised on config error"
    pass

class ReadError(Exception):
    "Raised on problem with the test's read"

class RunError(Exception):
    "Raised on problem with running test"

class CMIPRead:
    def __init__(self, config):
        self.config = config
        self.results = {'test': '{}-{}-{}'.format(config['method'],
        config['source'], config['read_pattern']),
                        'config_name': config['config_name'],
                        'run_at': time.ctime(),
                        'config_file': config['config_file'],
                        'config_modified_at': time.ctime(os.stat(config['config_file']).st_mtime)  if config['config_file'] else None,
                        'repeats': config['repeats'],
                        'read_pattern': config['read_pattern']
                        }

        

    def get_zarr_store(self):
        caringo_s3 = s3fs.S3FileSystem(anon=True,
                        client_kwargs={'endpoint_url': self.config['endpoint']})
        zarr_path = os.path.join(self.config['path'], self.config['file'])
        store = s3fs.S3Map(root=zarr_path, s3=caringo_s3)   
        return store            

        
    def save_results(self):
        # check if there's a pickled dataframe already on disk
        logging.info('Saving data...')
        try:
            df = pd.read_json('results_df.json')
        except ValueError:
            df =pd.DataFrame()

        df = df.append(self.results, ignore_index=True)

        # save to disk
        df.to_json('results_df.json')

    def _check_s3_nc_path(self, fp):
        if fp.startswith('http'):
            fp += '#mode=bytes'
        return fp
   
    def read_nc_serial(self):
        fp = os.path.join(self.config['path'], self.config['file'])
        # if S3 url add #mode=bytes to fp
        fp = self._check_s3_nc_path(fp)
        total_bytes = 0
        for ir in range(self.config['repeats']):
            nc = Dataset(fp,'r')
            var = nc.variables[config['var']]
            for i in range(var.shape[0]):
                logging.debug('Index: {}'.format(i))
                data = var[i,:,:,:]
                total_bytes += data.nbytes

            nc.close()

        return total_bytes

    def read_nc_map(self):
        fp = os.path.join(self.config['path'], self.config['file'])
        # if S3 url add #mode=bytes to fp
        fp = self._check_s3_nc_path(fp)
        total_bytes = 0
        for ir in range(self.config['repeats']):
            nc = Dataset(fp,'r')
            var = nc.variables[config['var']]
            rx = randint(0, var.shape[0]-1)
            ry = randint(0, var.shape[1]-1)
            logging.debug('Index: [{},{},:,:]'.format(rx,ry))
            data = var[rx,ry,:,:]
            total_bytes += data.nbytes

            nc.close()

        return total_bytes

    def read_nc_timeseries(self):
        fp = os.path.join(self.config['path'], self.config['file'])
        # if S3 url add #mode=bytes to fp
        fp = self._check_s3_nc_path(fp)
        total_bytes = 0
        for ir in range(self.config['repeats']):
            nc = Dataset(fp,'r')
            var = nc.variables[config['var']]
            rx = randint(0, var.shape[1]-1)
            ry = randint(0, var.shape[2]-1)
            logging.debug('Index: [:,{},{},:]'.format(rx,ry))
            data = var[:,rx,ry,0]
            total_bytes += data.nbytes

            nc.close()

        return total_bytes

    def read_s3nc_serial(self):
        # S3netcdf serial read method
        fp = os.path.join(self.config['path'], self.config['file'])
        total_bytes = 0
        for ir in range(self.config['repeats']):
            nc = s3Dataset(fp,'r')
            var = nc.variables[config['var']]
            for i in range(var.shape[0]):
                logging.debug('Index: {}'.format(i))
                data = var[i,:,:,:]
                total_bytes += data.nbytes

            nc.close()

        return total_bytes

    def read_s3nc_map(self):
        # s3netcdf map read
        fp = os.path.join(self.config['path'], self.config['file'])
        total_bytes = 0
        for ir in range(self.config['repeats']):
            nc = s3Dataset(fp,'r')
            var = nc.variables[config['var']]
            rx = randint(0, var.shape[0]-1)
            ry = randint(0, var.shape[1]-1)
            logging.debug('Index: [{},{},:,:]'.format(rx,ry))
            data = var[rx,ry,:,:]
            total_bytes += data.nbytes

            nc.close()

        return total_bytes

    def read_s3nc_timeseries(self):
        #s3netcdf time series read
        fp = os.path.join(self.config['path'], self.config['file'])
        total_bytes = 0
        for ir in range(self.config['repeats']):
            nc = s3Dataset(fp,'r')
            var = nc.variables[config['var']]
            rx = randint(0, var.shape[1]-1)
            ry = randint(0, var.shape[2]-1)
            logging.debug('Index: [:,{},{},:]'.format(rx,ry))
            data = var[:,rx,ry,0]
            total_bytes += data.nbytes
            logging.debug('shape of results = {}'.format(data.shape))

            nc.close()

        return total_bytes

    def read_zarr_map(self):
        store = self.get_zarr_store()
        total_bytes = 0
        for ir in range(self.config['repeats']):            
            # open dataset
            ds = xr.open_zarr(store=store, consolidated=True)
            var = ds[self.config['var']]
            rx = randint(0, var.shape[0]-1)
            ry = randint(0, var.shape[1]-1)
            logging.debug('Index: [{},{},:,:]'.format(rx,ry))
            data = var[rx,ry,:,:].load()
            total_bytes += data.nbytes

            ds.close()

        return total_bytes

    def read_zarr_timeseries(self):
        store = self.get_zarr_store()
        total_bytes = 0
        for ir in range(self.config['repeats']):            
            # open dataset
            ds = xr.open_zarr(store=store, consolidated=True)
            var = ds[self.config['var']]
            rx = randint(0, var.shape[1]-1)
            ry = randint(0, var.shape[2]-1)
            logging.debug('Index: [:,{},{},:]'.format(rx,ry))
            data = var[:,rx,ry,0].load()
            total_bytes += data.nbytes

            ds.close()

        return total_bytes

    def read_zarr_serial(self):
        store = self.get_zarr_store()
        total_bytes = 0
        for ir in range(self.config['repeats']):            
            # open dataset
            ds = xr.open_zarr(store=store, consolidated=True)
            var = ds[self.config['var']]
            for i in range(var.shape[0]):
                logging.debug('Index: {}'.format(i))
                data = var[i,:,:,:].load()
                total_bytes += data.nbytes


            ds.close()

        return total_bytes

    def run(self):
        logging.info('Starting test...')
        logging.debug('config being used: {}'.format(self.config))
        # start timer 
        if self.config['repeats'] > 1:
            raise RunError('Repeats ({}) greater than 1 not implemented because of caching file.'.format(self.config['repeats']))

        start_time = time.time()
        
        logging.info('Reading using {} from {}...'.format(config['method'],config['source']))
        # work out which version of the test to run
        if self.config['method'] == 'netCDF4-python':
            if self.config['read_pattern'] == 'serial':
                bytes_read = self.read_nc_serial()
            elif self.config['read_pattern'] == 'map':
                bytes_read = self.read_nc_map()
            elif self.config['read_pattern'] == 'timeseries':
                bytes_read = self.read_nc_timeseries()
            else:
                bytes_read = None
        elif self.config['method'] == 'S3netCDF4-python':
            if self.config['read_pattern'] == 'serial':
                bytes_read = self.read_s3nc_serial()
            elif self.config['read_pattern'] == 'map':
                bytes_read = self.read_s3nc_map()
            elif self.config['read_pattern'] == 'timeseries':
                bytes_read = self.read_s3nc_timeseries()
            else:
                bytes_read = None
        elif self.config['method'] == 'zarr':
            if self.config['read_pattern'] == 'serial':
                bytes_read = self.read_zarr_serial()
            elif self.config['read_pattern'] == 'map':
                bytes_read = self.read_zarr_map()
            elif self.config['read_pattern'] == 'timeseries':
                bytes_read = self.read_zarr_timeseries()
            else:
                bytes_read = None
        else: 
            raise ConfigError('Test config invalid, check "source" and "method": \n{}'.format(self.config))

        total_time = time.time() - start_time
        # rate of read in MB/s
        rateMB = bytes_read/total_time/10**6
        self.results['total_time'] = total_time
        self.results['rateMB'] = rateMB
        self.results['bytes_read'] = bytes_read

        logging.debug('Bytes Read: {}\nTime taken: {}\nRate: {}'.format(bytes_read,total_time, rateMB))

        self.save_results()


if __name__=="__main__":
    args = parser.parse_args()

    if args.config:
        config = json.load(open(args.config))
        config['config_file'] = args.config
    else:
        config = {'config_name':'default',
                  'source': 'disk', 
                  'method': 'netCDF4-python',
                  'path': 'default',
                  'log_level': 'debug', 
                  'repeats': 1, 
                  'config_file': None,
                  'file': 'test0-1.nc',
                  'var': 'var',
                  'read_pattern': 'serial',
                  'path': '/gws/nopw/j04/perf_testing/cmip6'
                  }

    # activate the right venv and import libraries
    if config['method'] == 'netCDF4-python':
        activate_this_file = "/home/users/mjones07/cmip-venv-nc/bin/activate_this.py"
        exec(open(activate_this_file).read(), {'__file__': activate_this_file})
        from netCDF4 import Dataset
    elif config['method'] == 'S3netCDF4-python':
        activate_this_file = "/home/users/mjones07/s3nc_venv/bin/activate_this.py"
        exec(open(activate_this_file).read(), {'__file__': activate_this_file})
        from S3netCDF4._s3netCDF4 import s3Dataset
    elif config['method'] == 'zarr':
        activate_this_file = "/home/users/mjones07/cmip-zarr-new/bin/activate_this.py"
        exec(open(activate_this_file).read(), {'__file__': activate_this_file})
        import xarray as xr
        import s3fs
    else:
        raise ImportError('libs not imported')

    logger_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=logger_format, level=args.log_level.upper())

    cmip_read = CMIPRead(config)
    cmip_read.run()
