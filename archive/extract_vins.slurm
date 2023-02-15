#!/bin/bash

#### Job Name ####
#SBATCH --job-name=vins

#### Account and Partition ####
#SBATCH --account=p_dsi
#SBATCH --partition=production

#### Resources Required ####
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=12:00:00

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
module load GCC/10.2.0 OpenMPI/4.0.5
module load SciPy-bundle/2020.11 IPython/7.18.1

python extract_vins.py
