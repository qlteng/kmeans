model=kmeans_init
HADOOP_CMD=/opt/tiger/yarn_deploy/hadoop/bin/hadoop
dir=`dirname $0`
$SPARK_HOME/bin/spark-submit \
  --class AwemeUserBIExtract \
  --master yarn-client \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.initialExecutors=500 \
  --conf spark.dynamicAllocation.minExecutors=250 \
  --conf spark.dynamicAllocation.maxExecutors=1000 \
  --executor-memory 48g \
  gen_int_cluster.py  "k=$1" "f=$2"\
  > ${dir}/log/${model}$1log 2>&1
wait
$HADOOP_CMD fs -text /user/tengqianli/simons_data/fork/clustertest$1/* >${dir}/data/cluster$1_1.txt
