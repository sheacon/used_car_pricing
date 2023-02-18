#!/bin/bash

#### Job Name ####
#SBATCH --job-name=gunzip

#### Account and Partition ####
#SBATCH --account=p_dsi
#SBATCH --partition=production

#### Resources Required ####
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --constraint=skylake
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

gzip -dc /data/p_dsi/capstone_projects/shea/mc_listings.csv.gz > /data/p_dsi/capstone_projects/shea/mc_listings.csv
