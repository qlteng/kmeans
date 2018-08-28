from ast import literal_eval as le
from pyspark import SparkContext,SparkConf
from ast import literal_eval as le
import sys,argparse
import math


if __name__=='__main__':
    reload(sys)
    arg_dict={}
    for arg in sys.argv[1:]:
        k,v=arg.split("=")
        arg_dict[k]=v
    print arg_dict

    conf=SparkConf()
    conf.setAppName("samples10000")
    conf.set("spark.executor.memory", "48g")
    conf.set("spark.driver.memory", "24g")
    conf.set("spark.memory.offHeap.size","48g")
    sc = SparkContext(conf=conf)

    data = sc.textFile("/user/tengqianli/simons_data/cid_img_title_vectors")
    #data = sc.textFile("/user/tengqianli/simons_data/subdata")
    raw_data = data.map(lambda x: (le(x.split('_')[1]) + le(x.split('_')[2])))
    cnt=raw_data.count()
    init_cluster=raw_data.sample(False,float(arg_dict['k'])/cnt,122)
    index_cluster=init_cluster.zipWithIndex().map(lambda x:(x[1],x[0]))
    path="/user/tengqianli/simons_data/fork/clustertest%s"%arg_dict['k']
    index_cluster.saveAsTextFile(path)
