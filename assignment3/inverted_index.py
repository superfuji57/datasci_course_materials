# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 17:09:42 2014

@author: andrewon
"""
import MapReduce
import sys

# Part 1
mr = MapReduce.MapReduce()

# Part 2
def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
      mr.emit_intermediate(w, key)

# Part 3
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    filenames = set()
    for v in list_of_values:
      filenames.add(v)
    mr.emit((key, [f for f in filenames]))

# Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
