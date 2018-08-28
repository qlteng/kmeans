#!/bin/bash

mapred_min_split_size=100000000
mapred_reduce_tasks=1000

dir=`dirname $0`
InputPath="/user/tengqianli/simons_data/cid_img_title_vectors"
#InputPath="/user/tengqianli/simons_data/subdata"
HADOOP_CMD=/opt/tiger/yarn_deploy/hadoop/bin/hadoop
HADOOP_STREAMING_JAR="/opt/tiger/yarn_deploy/hadoop-2.6.0-cdh5.4.4/share/hadoop/tools/lib/hadoop-streaming-2.6.0-cdh5.4.4.jar"
end=3

for((iter=3;iter<=$end;iter=iter+1))
	do
	iter_1=$(($iter+1))
	echo $1_$iter_1
	OutputPath="/user/tengqianli/simons_data/fork2/cluster$1_${iter_1}"

	$HADOOP_CMD jar $HADOOP_STREAMING_JAR \
    -libjars /opt/tiger/yarn_deploy/hadoop-2.6.0-cdh5.4.4/bytedance-data-1.0.1.jar \
    -Dmapreduce.job.priority=CRITICAL \
    -Dmapreduce.job.name=kmeansmr_tengqianli_$1_$iter\
    -Dmapreduce.reduce.memory.mb=8000  \
    -Dmapreduce.map.memory.mb=8000  \
    -Dmapreduce.input.combinefileformat.tasks=10000 \
    -Dmapreduce.reduce.speculative=false \
    -Dmapred.child.java.opts=-Xmx2000m \
    -Dstream.map.input.ignoreKey=true \
    -Dmapreduce.input.fileinputformat.split.minsize=${mapred_min_split_size} \
    -Dmapreduce.job.reduces=${mapred_reduce_tasks} \
    -Dmapreduce.output.fileoutputformat.compresss=false \
    -Dmapreduce.task.timeout=18000000 \
    -Dmapreduce.job.queue.name="data.ad" \
    -inputformat com.bytedance.data.CustomCombineFileInputFormat \
    -input $InputPath \
    -output $OutputPath \
    -file "${dir}/mapper_kmeans.py" \
    -file "${dir}/data/cluster$1_${iter}.txt" \
    -mapper "python mapper_kmeans.py --mr m --t cluster$1_${iter}.txt" \
    -reducer "python mapper_kmeans.py --mr r" \
    > ${dir}/log/cluster$1_${iter}.log 2>&1
	wait	
	$HADOOP_CMD fs -text ${OutputPath}/* >${dir}/data/cluster$1_${iter_1}.txt
	wait 
done
wait
end=$(($end+1))

OutputPath="/user/tengqianli/simons_data/fork2/sse_res_$1"

$HADOOP_CMD jar $HADOOP_STREAMING_JAR \
-libjars /opt/tiger/yarn_deploy/hadoop-2.6.0-cdh5.4.4/bytedance-data-1.0.1.jar \
-Dmapreduce.job.priority=CRITICAL \
-Dmapreduce.job.name=kmeans_sse_tengqianli$1\
-Dmapreduce.reduce.memory.mb=8000  \
-Dmapreduce.map.memory.mb=8000  \
-Dmapreduce.input.combinefileformat.tasks=10000 \
-Dmapreduce.reduce.speculative=false \
-Dmapred.child.java.opts=-Xmx2000m \
-Dstream.map.input.ignoreKey=true \
-Dmapreduce.input.fileinputformat.split.minsize=${mapred_min_split_size} \
-Dmapreduce.job.reduces=${mapred_reduce_tasks} \
-Dmapreduce.output.fileoutputformat.compresss=false \
-Dmapreduce.task.timeout=18000000 \
-Dmapreduce.job.queue.name="data.ad" \
-inputformat com.bytedance.data.CustomCombineFileInputFormat \
-input $InputPath \
-output $OutputPath \
-file "${dir}/cal_sse.py" \
-file "${dir}/data/cluster$1_${end}.txt" \
-mapper "python cal_sse.py --mr m --t cluster$1_${end}.txt" \
-reducer "python cal_sse.py --mr r" \
> ${dir}/log/sse.log$1 2>&1
wait
$HADOOP_CMD fs -text ${OutputPath}/* >${dir}/data/sse_res_$1.txt
wait
python ${dir}/cal_k_sse.py --f ${dir}/data/sse_res_$1.txt --k $1 --o ${dir}/data/ --e "extra"
