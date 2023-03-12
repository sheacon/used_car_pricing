q#!/bin/bash
#SBATCH --mail-user=shea.conaway@vanderbilt.edu
#SBATCH --mail-type=ALL
#SBATCH --time=2:00:00
#SBATCH --err=err.out
#SBATCH --out=out.out
#SBATCH --account=p_dsi
#SBATCH --cpus-per-task=2
#SBATCH --array=0-16701%20


module load GCC Python
pip install pyarrow --user

basedir="/data/p_dsi/capstone_projects/shea/0_processed/full_data"

arrayfile=`ls $basedir | awk -v line=$SLURM_ARRAY_TASK_ID '{if (NR == line) print $0}'`

outdir="${arrayfile%.parquet}"

/home/conawws1/used_car_pricing/process_listings/1_partition_python.py $basedir/$arrayfile $SLURM_ARRAY_TASK_ID



