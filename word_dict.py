import sys
import csv
import json
import pickle
"""creates a simplified CSV file containing only words that are either positive or negative"""

"""argv[1] is the filepath of the Loughran sentiment dictionary"""

"""argv[2] is the destination filepath for the written csv file"""

"""note that I first edited the original Loughran  dataset to include only 'Words','Negative', and 'Positive' entries"""

with open('/Users/jonathandriscoll/PycharmProjects/ProjectC/posneg.json','r') as infile:
    posneg=json.load(infile)
kargs[1]





with open('/Users/jonathandriscoll/PycharmProjects/ProjectC/test.txt','w') as outfile:
    pickle.dumps(posneg, outfile)




"""create JSON file from .csv"""






