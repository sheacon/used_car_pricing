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
#SBATCH --constraint=cascadelake
#SBATCH --mem=8G
#SBATCH --time=24:00:00

#### Environment Related ####
#SBATCH --export=ALL

#### Output and Error Information ####
#SBATCH --output=%x.out
#SBATCH --error=%x.err

#### Email Notification ####
#SBATCH --mail-user=shea.conaway@vanderbilt.edu
#SBATCH --mail-type=ALL

MAX_FIELD_LEN=9223372036854775807

INPUT_FILE=/data/p_dsi/capstone_projects/shea/mc_listings.csv
OUTPUT_FILE=line_check.csv

csvclean -n -z $MAX_FIELD_LEN $INPUT_FILE > $OUTPUT_FILE

#INPUT_FILE=/data/p_dsi/capstone_projects/shea/mc_listings.csv
#OUTPUT_FILE=/scratch/conawws1/vin_date.csv
#csvcut -c vin,scraped_at,status_date,first_scraped_at -l -z $MAX_FIELD_LEN $INPUT_FILE > $OUTPUT_FILE
# Error: line contains NULL byte
# line 77,744,993 +/-
