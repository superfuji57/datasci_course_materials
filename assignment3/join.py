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
    # key: order_id
    # value: columns of record
    typ = record[0] #type
    key = record[1]
    mr.emit_intermediate(key, record)
    

def reducer(key, list_of_values):
    # key: order_id
    # value: list of orders, lineitems with that key
    order = list_of_values[0]
    for line_item in list_of_values[1:]:
      mr.emit(order + line_item)
    print (key, order + line_item)

# Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
