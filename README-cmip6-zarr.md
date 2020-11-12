# Notes on cmip6 zarr work

## How do we decide on the chunk size?

We did some testing of efficient object sizes in our object store (Caringo) -
it suggested 100Mb - 1Gb was the optimum size. So we have set 250Mb as our
target value. Depending on the array shape, our chunks should come in around
250Mb.

## Which data frequencies have we covered?

So far, we have the following in our store:

AERday, Amon, CFday, day, Eday, LImon, Lmon, Oday, Omon, Primday

## Why develop a package rather than a notebook application?

I see this task as a batch processing task. I want to be able to
say "run everything" and let the task manage itself. This is very
hard to do, so in reality I need lots of ways to catch failures etc.

These include:
 - modifying the number of HTTP connections in the pool:
   https://github.com/cedadev/cmip6-object-store/blob/master/cmip6_object_store/cmip6_zarr/caringo_store.py#L21
 - doing retries on a number of operations, to overcome temporary failures:
   https://github.com/cedadev/cmip6-object-store/blob/master/cmip6_object_store/cmip6_zarr/caringo_store.py#L24
 - limiting the total memory used so that each task will fit into the
   memory limit on each batch node:
   https://github.com/cedadev/cmip6-object-store/blob/master/cmip6_object_store/etc/config.ini#L32 (this is how we avoid memory errors)
 - each variable is processed separately to keep the volumes relatively
   low writing all successes to an internal catalogue (i.e. log), so that
   the framework will not rerun tasks that were previously successful:
   https://github.com/cedadev/cmip6-object-store/blob/master/cmip6_object_store/cmip6_zarr/zarr_writer.py#L122-L124
 - log failures to a separate catalogue (log) so that they can be interrogated
   and run interactively for debugging:
   https://github.com/cedadev/cmip6-object-store/blob/master/cmip6_object_store/cmip6_zarr/zarr_writer.py#L126-L130

Overall, in my view, this kind of large-scale processing does not fit well with notebooks.
I would rather produce a notebook to interact with the Zarr store, on the user side:

https://github.com/cedadev/cmip6-object-store/blob/master/notebooks/cmip6-zarr-jasmin.ipynb
