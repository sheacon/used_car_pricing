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
#SBATCH --mem=32G
#SBATCH --time=03:00:00
#SBATCH --array=1-100%20
#SBATCH --constraint=skylake|cascadelake

#### Output ####
#SBATCH --export=ALL
##SBATCH --output=%x_%j.out
##SBATCH --error=%x_%j.err

module restore default

subfolder=$SLURM_ARRAY_TASK_ID

python /home/conawws1/used_car_pricing/process_listings/1_partition.py $subfolder


