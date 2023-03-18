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
#SBATCH --cpus-per-task=50
#SBATCH --mem=700G
#SBATCH --time=12:00:00
#SBATCH --array=0-2

#### Output ####
#SBATCH --export=ALL
##SBATCH --output=%x_%j.out
##SBATCH --error=%x_%j.err

module restore default

python /home/conawws1/used_car_pricing/process_listings/3_final_listings.py $SLURM_ARRAY_TASK_ID
