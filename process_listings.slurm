#!/bin/bash

#### Job Name ####
#SBATCH --job-name=process_listings

#### Account and Partition ####
#SBATCH --account=p_dsi
#SBATCH --partition=production

#### Resources Required ####
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=24:00:00
#SBATCH --array=0-127%32

#### Environment Related ####
#SBATCH --export=ALL

#### Output and Error Information ####
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err

#### Email Notification ####
#SBATCH --mail-user=shea.conaway@vanderbilt.edu
#SBATCH --mail-type=ALL

# accre packages
module purge # confirm clean env
module load GCC/10.2.0 OpenMPI/4.0.5 SciPy-bundle/2020.11 IPython/7.18.1

python process_listings.py 224121706 128 $SLURM_ARRAY_TASK_ID