#!/bin/bash

#### Job Name ####
#SBATCH --job-name=tx_dd

#### Accounts ####
#SBATCH --account=p_dsi
#SBATCH --partition=production
#SBATCH --mail-user=shea.conaway@vanderbilt.edu
#SBATCH --mail-type=ALL

#### Resources Required ####
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=100G
#SBATCH --time=03:00:00
#SBATCH --array=0-4
#SBATCH --constraint=skylake

#### Output ####
#SBATCH --export=ALL
##SBATCH --output=%x_%j.out
##SBATCH --error=%x_%j.err

module restore default

state="TX"

python /home/conawws1/used_car_pricing/process_listings/2_dedupe.py $state $SLURM_ARRAY_TASK_ID
