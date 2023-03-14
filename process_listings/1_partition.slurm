#!/bin/bash

#### Job Name ####
##SBATCH --job-name=

#### Accounts ####
#SBATCH --account=p_dsi
#SBATCH --partition=production
#SBATCH --mail-user=shea.conaway@vanderbilt.edu
#SBATCH --mail-type=ALL

#### Resources Required ####
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=32G
#SBATCH --time=06:00:00
##SBATCH --array=1-100%20
#SBATCH --constraint=skylake|cascadelake

#### Output ####
#SBATCH --export=ALL
##SBATCH --output=%x_%j.out
##SBATCH --error=%x_%j.err


module restore default

array_size=100

#python /home/conawws1/used_car_pricing/process_listings/1_partition.py $SLURM_ARRAY_TASK_ID $array_size
python /home/conawws1/used_car_pricing/process_listings/1_partition.py 1 $array_size


