#!/usr/bin/python

import sys, re, math,random,os
import argparse
from ast import literal_eval as le


def read_from_clusters_cache_file(clusters_file):
    f = open(clusters_file, 'r')
    data = f.read()
    f.close()
    del f
    return data


def read_clusters(CLUSTERS_FILENAME):
    cluster_data = read_from_clusters_cache_file(CLUSTERS_FILENAME)

    for line in cluster_data.strip().split("\n"):
        centroid_id=le(line)[0]
        coords=le(line)[1]
        clusters.append((centroid_id, coords))


def get_distance_coords(vec1,vec2):
    dist = 0
    for i in range(len(vec1)):
        dist += math.sqrt(math.pow(vec1[i] - vec2[i],2))
    return dist


def get_nearest_cluster(vector):
    nearest_cluster_id = None
    nearest_distance = 1000000000
    for cluster in clusters:
        dist = get_distance_coords(vector, cluster[1])
        if dist < nearest_distance:
            nearest_cluster_id = cluster[0]
            nearest_distance = dist
    return nearest_cluster_id,nearest_distance


def vecplus(vec1,vec2):
    temp = []
    for i in range(len(vec1)):
        temp.append(vec1[i]+vec2[i])
    return temp


def mapper():
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        ws = line.strip('\n').split('_')
        key = ws[0]
        value=le(ws[1])+le(ws[2])
        #value = le(ws[1])[:NUM]
        nearest_cluster_id,nearest_cluster_dist = get_nearest_cluster(value)
        print str(nearest_cluster_id)+"\t"+str(key)+"_"+str(nearest_cluster_dist)


def avesum(vec,cnt):
    temp=[]
    for i in range(len(vec)):
        temp.append(vec[i]/cnt)
    return temp


def reducer():
    oldKey = None
    sse_total = 0
    cidlist=[]
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        ws = line.strip().split('\t')
        if len(ws)!=2:
            continue
        cluster_id, totals = ws
        cid,sse =totals.split("_")
        sse=float(sse)
        if oldKey and oldKey != cluster_id:
            print str(oldKey)+"\t"+str(sse_total)+"_"+str(cidlist)
            sse_total=0
            cidlist=[]
        oldKey = cluster_id
        sse_total+=sse
        cidlist.append(cid)
    print str(oldKey)+"\t"+str(sse_total)+"_"+str(cidlist)


def main(args):
    op_type = ''
    if args.mr is None:
        return
    else:
        op_type = args.mr

    if op_type == 'm':
        CLUSTERS_FILENAME =args.t
        # CLUSTERS_FILENAME = 'resultcenter.txt'
        read_clusters(CLUSTERS_FILENAME)
        mapper()
    if op_type == 'r':
        reducer()


if __name__ == '__main__':
    NUM=1152
    parser = argparse.ArgumentParser(description='kmeans_mr')
    parser.add_argument('--mr', type=str, help='MR')
    parser.add_argument('--t', type=str, help='iterators')
    args = parser.parse_args()
    clusters = []

    main(args)


