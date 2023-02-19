#!/bin/bash

#SBATCH --job-name=example
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
#SBATCH --mail-user=shea.conaway@vanderbilt.edu
#SBATCH --mail-type=ALL
#SBATCH --export=ALL

#SBATCH --account=p_dsi
#SBATCH --partition=production

#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --constraint=skylake|cascadelake
#SBATCH --mem=8G
#SBATCH --time=01:30:00

# accre modules
module purge # confirm clean env
module restore # load defaults

echo 'Hello World!'
