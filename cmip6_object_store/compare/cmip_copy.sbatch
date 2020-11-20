#!/bin/bash
#SBATCH --time 6:00:00
#SBATCH -q short-serial
#SBATCH -o /home/users/mjones07/cmip_copy.log


#module load jaspy
#ncrcat /badc/cmip6/data/CMIP6/CMIP/BCC/BCC-CSM2-MR/piControl/r1i1p1f1/Amon/hur/gn/v20181016/* /gws/nopw/j04/perf_testing/cmip6/CMIP.BCC.BCC-CSM2-MR.piControl.r1i1p1f1.Amon.hur.gn.v20181016.nc

s3cmd put /gws/nopw/j04/perf_testing/cmip6/CMIP.BCC.BCC-CSM2-MR.piControl.r1i1p1f1.Amon.hur.gn.v20181016.nc s3://mjones07/CMIP.BCC.BCC-CSM2-MR.piControl.r1i1p1f1.Amon.hur.gn.v20181016.nc

