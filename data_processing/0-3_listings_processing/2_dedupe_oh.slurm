#!/bin/bash

#### Job Name ####
#SBATCH --job-name=oh_dd

#### Accounts ####
#SBATCH --account=p_dsi
#SBATCH --partition=production
#SBATCH --mail-user=shea.conaway@vanderbilt.edu
#SBATCH --mail-type=ALL

#### Resources Required ####
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=240G
#SBATCH --time=03:00:00
#SBATCH --array=0-6

#### Output ####
#SBATCH --export=ALL
##SBATCH --output=%x_%j.out
##SBATCH --error=%x_%j.err

module restore default

state="OH"

python /home/conawws1/used_car_pricing/process_listings/2_dedupe.py $state $SLURM_ARRAY_TASK_ID
