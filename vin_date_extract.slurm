#!/bin/bash

#### Job Name ####
#SBATCH --job-name=vin_date_extract

#### Account and Partition ####
#SBATCH --account=p_dsi
#SBATCH --partition=production

#### Resources Required ####
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --constraint=skylake|cascadelake
#SBATCH --mem=8G
#SBATCH --time=24:00:00

#### Environment Related ####
#SBATCH --export=ALL

#### Output and Error Information ####
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err

#### Email Notification ####
#SBATCH --mail-user=shea.conaway@vanderbilt.edu
#SBATCH --mail-type=ALL

csvcut -c vin,scraped_at,status_date,first_scraped_at -l -z 500000 /data/p_dsi/capstone_projects/shea/mc_listings.csv > /scratch/conawws1/vin_date_extract.csv
