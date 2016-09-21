#!/usr/bin/python

from multiprocessing import Process, Pool
import os
import gzip
import random
import string
import pdb
import glob
import itertools
import fileinput

UNPROCESSED_DIR = "./unprocessed/"
PROCESSED_DIR = "./processed/"
def create_files():
    for i in range(10):
        fh = gzip.open(UNPROCESSED_DIR + "file_" + str(i) + ".json.gz", "w")
        for j in range(100000):
            fh.write(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(100)))
        fh.close()

def process_file(file):
    filename = os.path.split(file)[1]
    output = gzip.open(PROCESSED_DIR + "processed_" + filename,"w")
    i = 0
    for line in fileinput.FileInput([file],openhook=fileinput.hook_compressed):
        output.write(line + "processed " + str(i))
        i += 1
    output.close()
        
    return "Processed " + str(file)

    
if __name__=="__main__":
    create_files()
    files = glob.glob("./unprocessed/*.gz")
    
    pool = Pool(processes=4)
    #pdb.set_trace()
    for p in pool.imap(process_file, files):
        print p

