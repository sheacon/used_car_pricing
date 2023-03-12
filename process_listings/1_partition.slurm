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
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=4G
#SBATCH --time=00:05:00
#SBATCH --array=0-16701%5

#### Output ####
#SBATCH --export=ALL
##SBATCH --output=%x_%j.out
##SBATCH --error=%x_%j.err


module restore default

input_dir="/data/p_dsi/capstone_projects/shea/0_processed/full_data/"
output_dir="/data/p_dsi/capstone_projects/shea/1_partitioned_array/"

file=`ls $input_dir | awk -v line=$SLURM_ARRAY_TASK_ID '{if (NR == line) print $0}'`

python /home/conawws1/used_car_pricing/process_listings/1_partition.py $input_dir $file $output_dir



