#!/bin/bash
#
# get environment variables
GLOBAL_RANK=$SLURM_PROCID
CPUS=$SLURM_CPUS_PER_TASK
MEM=$SLURM_MEM_PER_NODE # seems to be in MB
MASTER_ADDR=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)
LOCAL_IP=$(hostname -I | awk '{print $1}')

# set some environment variables for the indexing script
export MASTER_ADDR=$MASTER_ADDR
export MEMORY=$MEM
export CPUS=$CPUS
export SPARK_LOCAL_IP=$LOCAL_IP

# setup the master node
if [ $GLOBAL_RANK == 0 ]
then
    # print out some info
    echo -e "MASTER ADDR: $MASTER_ADDR\tGLOBAL RANK: $GLOBAL_RANK\tCPUS PER TASK: $CPUS\tMEM PER NODE: $MEM"

    # then start the spark master node in the background
    ./spark-3.3.1-bin-hadoop3/sbin/start-master.sh -p 7079 -h $LOCAL_IP
   
fi

sleep 10

# then start the spark worker node in the background
MEM_IN_GB=$(($MEM / 1000))
# concat a "G" to the end of the memory string
MEM_IN_GB="$MEM_IN_GB"G
echo "MEM IN GB: $MEM_IN_GB"

export SPARK_LOCAL_DIRS=/scratch/local
export SPARK_WORKER_DIR=/scratch/work

./spark-3.3.1-bin-hadoop3/sbin/start-worker.sh -c $CPUS -m $MEM_IN_GB "spark://$MASTER_ADDR:7079"
echo "Hello from worker $GLOBAL_RANK"


sleep 10

if [ $GLOBAL_RANK == 0 ]
then
    # then start some script
    echo "hi"
fi

sleep 10000
