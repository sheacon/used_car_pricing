#!/bin/bash

#### Job Name ####
#SBATCH --job-name=example

#### Account and Partition ####
#SBATCH --account=p_dsi
#SBATCH --partition=production

#### Resources Required ####
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --constraint=skylake
#SBATCH --mem=8G
#SBATCH --time=01:30:00

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
#module restore bigdata

echo 'Hello World!'
