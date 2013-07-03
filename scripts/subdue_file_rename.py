import sys
import os

rootdir = sys.argv[1]
outputdir = sys.argv[2]
dataset = sys.argv[3]

for root, subfolders, files in os.walk(rootdir):
    for filename in files:
        if filename.startswith(dataset):
            new_filename = filename.replace('-r-00000', '') + '.g'
            dataset_filename = new_filename[0: len(dataset)]
            number = new_filename[len(dataset) + 1: len(new_filename)]
            new_filename = dataset_filename + '_' + number
            f_source = open(root + '/' + filename)
            f_target = open(outputdir + '/' + new_filename, 'w')
            f_target.write(f_source.read())
            f_target.close()
            f_source.close()
