#!/usr/bin/python

import sys, re, math,random,os
import argparse
from ast import literal_eval as le




def main(args):
    sse=0
    f=open(args.f,'r')
    fw=open(args.o+"ssefork_%s.txt"%args.k,'w')
    for line in f:
        sse+=float(line.split('\t')[1].split('_')[0])
    fw.write(str(args.k)+"\t"+str(sse))
    fw.close()
    f.close()




if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='cal_k_sse')
    parser.add_argument('--f', type=str, help='sse')
    parser.add_argument('--k', type=str, help='clusters')
    parser.add_argument('--e', type=str, help='extra')
    parser.add_argument('--o',type=str,help="output")
    args = parser.parse_args()

    main(args)


