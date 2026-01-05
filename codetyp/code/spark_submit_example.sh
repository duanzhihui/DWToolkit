#!/bin/bash
# Spark Submit 示例脚本
# 演示通过spark-submit提交Spark作业

# 设置环境变量
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=/etc/hadoop/conf

# 基本提交方式
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 8g \
    --executor-cores 4 \
    --num-executors 10 \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.executor.memoryOverhead=2g \
    --conf spark.dynamicAllocation.enabled=true \
    --jars /path/to/extra.jar \
    /path/to/spark_example.py

# 使用spark2-submit (CDH/HDP)
spark2-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 4g \
    /path/to/spark_job.py \
    --input hdfs://namenode:8020/data/input \
    --output hdfs://namenode:8020/data/output

# 本地模式运行
spark-submit \
    --master local[4] \
    --driver-memory 2g \
    spark_example.py

# Kubernetes模式
spark-submit \
    --master k8s://https://kubernetes-master:6443 \
    --deploy-mode cluster \
    --conf spark.kubernetes.container.image=spark:latest \
    --conf spark.kubernetes.namespace=spark \
    local:///opt/spark/work-dir/spark_example.py
