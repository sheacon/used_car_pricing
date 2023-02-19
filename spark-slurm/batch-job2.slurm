#!/bin/bash
#SBATCH --job-name=spark-test
#SBATCH --account=p_dsi
#SBATCH --partition=production
#SBATCH --nodes=3
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=4G
#SBATCH --time=00:05:00
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
#SBATCH --mail-user=shea.conaway@vanderbilt.edu
#SBATCH --mail-type=ALL

# Load necessary modules
module load GCCcore/.8.2.0 Spark/2.4.0

# Environment variables
export SPARK_HOME=/accre/arch/easybuild/software/Compiler/GCCcore/8.2.0/Spark/2.4.0

# Start the Spark cluster
srun --label --multi-prog <(echo -e "0\tmaster\n1\tworker\n2\tworker") bash -c 'if [ $SLURM_PROCID -eq 0 ]; then $SPARK_HOME/sbin/start-master.sh; else $SPARK_HOME/sbin/start-worker.sh spark://$(hostname -f):7077; fi'

export PROJECT_DIR=/home/conawws1/used_car_pricing/spark-slurm/
export INPUT_FILE=/data/p_dsi/capstone_projects/shea/mc_listings.csv
export OUTPUT_FILE=/scratch/conawws1/spark_test.txt

# Submit the Spark job
$SPARK_HOME/bin/spark-submit \
  --master spark://$(hostname -f):7077 \
  --executor-memory 4g \
  --num-executors 2 \
  --executor-cores 4 \
 $PROJECT_DIR/SimpleApp.py $INPUT_FILE $OUTPUT_FILE

