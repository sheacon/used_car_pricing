#!/bin/bash

#### Job Name ####
##SBATCH --job-name=gunzip

#### Account and Partition ####
#SBATCH --account=p_dsi
#SBATCH --partition=production

#### Resources Required ####
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=4G
#SBATCH --time=6:00:00
#SBATCH --array=0-16701%20

#### Environment Related ####
#SBATCH --export=ALL

#### Output and Error Information ####
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err

#### Email Notification ####
#SBATCH --mail-user=shea.conaway@vanderbilt.edu
#SBATCH --mail-type=ALL

module restore default

input_dir="/data/p_dsi/capstone_projects/shea/0_processed/full_data/"
output_dir="/data/p_dsi/capstone_projects/shea/1_partitioned_python/"

file=`ls $input_dir | awk -v line=$SLURM_ARRAY_TASK_ID '{if (NR == line) print $0}'`

1_partition_python.py $input_dir $file $output_dir



