#! /usr/bin/python2.7

import csv
import json
import sys

csvread = csv.reader(open(sys.argv[1])) # dictionary csv for columns
# jsonstr = """{
#  "results": {
#        "tmp66": {
#               ".o_orderpriority__orders__o_orderpriority": [
#                       16,
#                       40,
#                       72,
#                       104,
#                       128
#                      ]
#              },
#        "tmp75": {
#               ".order_count": [
#                       311,
#                       263,
#                       266,
#                       274,
#                       36783
#                      ]
#              }
#       },
#   "timings": {
#         "timeInMicrosecondsForFragment12": 215,
#         "timeInMicrosecondsForFragment13": 565
#        } }"""

fulljson = json.load(sys.stdin) # json output from server
resolver = {}
for [tab,line,strng,code] in csvread:
    nm = ".".join([tab,line])
    if not resolver.has_key(nm):
        resolver[nm] = {}
    resolver[nm][int(code)] = strng
    
if not fulljson.has_key('results'):
    sys.stderr.write("no results available")

# {u'tmp66': {u'.o_orderpriority__orders__o_orderpriority': [16,
#                                                            40,
#                                                            72,
#                                                            104,
#                                                            128]},
#  u'tmp75': {u'.order_count': [311, 263, 266, 274, 36783]}}

results = fulljson['results']
outputcols = []
for res in results.values():
    if (len(res.keys()) != 1):
        sys.stderr.write('unexpected: more than one path in output')

    k = res.keys()[0]
    vals = res.values()[0]
    if vals == None:
        print >> sys.stderr, "WARNING: full column is null... continuing",  k
        vals = []
        
    names = k.split('__')

    if len(names) < 3:
        print >> sys.stderr, 'origin not know for column', k
        outputcols.append([k,vals])
        continue

    if len(names) > 3:
        print >> sys.stderr, 'name with more than 2 parts', k
        outputcols.append([k,vals])
        continue

    outputname = names[0]
    outputorigin = names[1:]
    dictname = ".".join(outputorigin)
    if not resolver.has_key(dictname):
        print >> sys.stderr, 'dictionary not found for ', dictname
        outputcols.append([k,vals])
        continue

    decoder= resolver[dictname]
    
    outputvals = []
    for v in vals:
        if not decoder.has_key(v):
            print >> sys.stderr, 'decoder has no mapping for: ', dictname, v
            outputvals.append(v)
        else:
            outputvals.append(decoder[v])

    outputcols.append([outputname, outputvals])


def make_padding(sz):
    return ['-' for i in range(sz)]


(names, vals) = zip(*outputcols)
maxlen = max([len(v) for v in vals])
paddedvals = [v + make_padding(maxlen - len(v)) for v in vals]
rowwise = zip(*paddedvals)
csvwr = csv.writer(sys.stdout)
csvwr.writerow(names)
csvwr.writerows(rowwise)

    
         
    

    

    
        
    


