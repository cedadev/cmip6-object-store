#!/bin/bash
#SBATCH -q par-single
#SBATCH -t 24:00:00
#SBATCH --exclusive
#SBATCH -o /home/users/mjones07/cmip_reads/submit_tests.out
#SBATCH -e /home/users/mjones07/cmip_reads/submit_tests.err


/home/users/mjones07/cmip_reads/submit_tests.sh