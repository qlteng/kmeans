k=1000
bash gen_int_cluster.sh $k "extra"
wait
bash kmeans_cluster.sh $k "extra"
wait
