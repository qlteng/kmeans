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
        if len(coords)!=NUM:
            continue
        clusters.append((centroid_id, coords))
        delta_clusters[centroid_id] = (list([0]*NUM),0)


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
    return nearest_cluster_id


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
        value = le(ws[1])+le(ws[2])
        if len(value)!=NUM:
            continue
        nearest_cluster_id = get_nearest_cluster(value)
        accsum, cont = delta_clusters[nearest_cluster_id]
        delta_clusters[nearest_cluster_id] = (vecplus(accsum,value), cont+1)
    for key in delta_clusters:
        accsum,cont = delta_clusters[key]
        print str(key) + "\t" + str(accsum)+";"+str(cont)


def avesum(vec,cnt):
    temp=[]
    for i in range(len(vec)):
        temp.append(vec[i]/cnt)
    return temp

def suggest_valid_coords_to_cluster():
    valid_clusters_count = len(valid_clusters)
    if valid_clusters_count <= 1:
        # Taking random values for a new coordinate
        new_center = get_random_coords_in_region()
    else:
        # Taking two clusters and positioning this on their average
        cid1 = random.randint(0, valid_clusters_count-1)
        cid2 = random.randint(0, valid_clusters_count-1)
        while cid1 == cid2:
            cid2 = random.randint(0, valid_clusters_count-1)

        cluster1 = valid_clusters[cid1]
        cluster2 = valid_clusters[cid2]
        new_center=avesum(vecplus(cluster1,cluster2),2)
    return new_center

def get_random_coords_in_region():


    return [random.uniform(-1,1) for x in range(NUM)]

def emit_new_lat_long(cluster_id, accsum_total, count_total):

    if count_total == 0: #if the cluster did not attracted any point, change to a new coord
        new_center = suggest_valid_coords_to_cluster()
        # return
    else:
        new_center=avesum(accsum_total,count_total)
        valid_clusters.append(new_center)
    print "("+str(cluster_id) + "," + str(new_center)+")"

def reducer():
    oldKey = None
    accsum_total = list([0]*NUM)
    count_total = 0

    while True:
        line = sys.stdin.readline()
        if not line:
            break
        ws = line.strip().split('\t')
        if len(ws)!=2:

            continue

        cluster_id, totals = ws
        accsum,count =totals.split(";")
        accsum=le(accsum)

        if oldKey and oldKey != cluster_id:
            emit_new_lat_long(oldKey, accsum_total,count_total)
            accsum_total = list([0]*NUM)
            count_total = 0

        oldKey = cluster_id
        accsum_total = vecplus(accsum_total,accsum)
        count_total += float(count)

        # print oldKey, accsum_total, count_total

    if oldKey != None:
        emit_new_lat_long(oldKey, accsum_total, count_total)



def main(args):
    op_type = ''
    if args.mr is None:
        return
    else:
        op_type = args.mr

    if op_type == 'm':
        CLUSTERS_FILENAME = args.t
        # CLUSTERS_FILENAME = 'twoclusteralltest.txt'
        read_clusters(CLUSTERS_FILENAME)
        mapper()
    if op_type == 'r':
        reducer()


if __name__ == '__main__':
    NUM=1152
    parser = argparse.ArgumentParser(description='kmeans_mr')
    parser.add_argument('--mr', type=str, help='MR')
    parser.add_argument('--t', type=str, help='cluster')
    args = parser.parse_args()
    clusters = []
    delta_clusters = dict()
    valid_clusters = []
    main(args)


