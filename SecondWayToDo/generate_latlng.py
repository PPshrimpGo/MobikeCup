# -*- coding: utf-8 -*-

import numpy as np
import csv
import os
import geohash

workspace = "./"
tmp_workspace = os.path.join(workspace,"slice_image/")

#将np中的字符变为数字
def parse_maybe_int(i):
    if i == '':
        return None
    else:
        return int(i)


filt_in = "test_geostart.txt"
file_output ='test_geostart_latlng.txt'
radar_echos = []
#

outputfile=open(file_output,"w")
i = 0
with open(filt_in,"r") as infile:
    for line in infile:
        context = line.split(",")
        #geohash.decode(context[0])
        print (context[0])
        print (geohash.decode(context[0]))
        outputfile.write(context[0])
        outputfile.write(',')
        geolatlng=geohash.decode_exactly(context[0])
        outputfile.write(str(geolatlng[0]))
        outputfile.write(',')
        outputfile.write(str(geolatlng[1]))
        outputfile.write(',')
        outputfile.write(str(geolatlng[2]))
        outputfile.write(',')
        outputfile.write(str(geolatlng[3]))
        outputfile.write('\n')

        #break
outputfile.close()