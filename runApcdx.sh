spark-submit --class apcdxTask\
    --deploy-mode client\
    --num-executors 5 \
    --executor-cores 2 \
    --executor-memory 2G \
    target/Task1-1.0-SNAPSHOT.jar