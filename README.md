# cmip6-object-store

## Introduction

[![Pypi](https://img.shields.io/pypi/v/cmip6-object-store.svg)](https://pypi.python.org/pypi/cmip6-object-store)

[![Travis](https://img.shields.io/travis/cedadev/cmip6-object-store.svg)](https://travis-ci.org/cedadev/cmip6-object-store)

[![Documentation](https://readthedocs.org/projects/cmip6-object-store/badge/?version=latest)](https://cmip6-object-store.readthedocs.io/en/latest/?badge=latest)

The CMIP6 Object Store Library holds a collection of different sub-packages for dealing with CMIP6 data in (the CEDA Caringo) Object Store:

 - `cmip6_zarr`: a library for batch-converting archived CMIP6 netCDF files to Zarr in Caringo.
 - `cmip6_ncrg`: a library for experimenting with the use of HTTP Range Gets to netCDF files in Caringo.
 - `cmip6_s3nc`: a library for experimenting with writing S3-netCDF files to Caringo.

* Free software: BSD - see LICENSE file in top-level package directory
* Documentation: https://cmip6-object-store.readthedocs.io.

## Package: cmip6_zarr

The `cmip6_zarr` package can be used as follows:

PRE-REQUISITE: Set up your caringo credentials as follows:

```
$ cat ~/.credentials/caringo-credentials.json
{
  "endpoint_url": "http://cmip6-zarr-o.s3.jc.rl.ac.uk/",
  "secret": "SECRET FROM CARINGO DASHBOARD",
  "token": "TOKEN FROM CARINGO DASHBOARD"
}
```

### Generate batches of CMIP6 datasets to process

```
python cmip6_object_store/cmip6_zarr/cli.py create-batches
```

### Run batch 1 on the local server

```
python cmip6_object_store/cmip6_zarr/cli.py run --project cmip6 --run-mode local --batches 1
```

### Run batches 1-100 on LOTUS

```
python cmip6_object_store/cmip6_zarr/cli.py run --project cmip6 --run-mode lotus --batches 1-100
```

NOTE: It _will not_ re-run batch 1 if you have already run it.

### Run all batches on LOTUS

```
python cmip6_object_store/cmip6_zarr/cli.py run --project cmip6 --run-mode lotus
```

### List all buckeet

```


## Credits

This package was created with `Cookiecutter` and the `audreyr/cookiecutter-pypackage` project template.

 * Cookiecutter: https://github.com/audreyr/cookiecutter
 * cookiecutter-pypackage: https://github.com/audreyr/cookiecutter-pypackage
