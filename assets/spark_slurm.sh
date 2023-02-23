#!/bin/bash
#SBATCH --partition=spark
#SBATCH --job-name=bild-cc-pile
#SBATCH --nodes 5
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task=48
#SBATCH --mem=0 # 0 means use all available memory (in MB)
#SBATCH --output=%x_%j.out
#SBATCH --comment laion
#SBATCH --exclusive

# wget https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz && tar xf spark-3.3.1-bin-hadoop3.tgz

# step 1: get environment variables
# step 2: setup rank 0 to be the master and start the indexing python file (the last two operations happen in parallel)
# step 3: start workers on all nodes

srun --comment laion bash worker_spark_on_slurm.sh
