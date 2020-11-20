#!/bin/bash


#hur_s3_zarr_serial.json hur_s3_nc_serial.json hur_s3_s3nc_serial.json
#hur_s3_nc_map.json hur_s3_s3nc_map.json hur_s3_nc_timeseries.json hur_s3_s3nc_timeseries.json
module load jaspy
for i in {0..10}; do

    for config in hur_s3_zarr_map.json  hur_s3_zarr_timeseries.json ; do
        python /home/users/mjones07/cmip_reads/cmip_read.py config/$config
    done
done
