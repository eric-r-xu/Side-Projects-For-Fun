# Eric Xu, 2018-11
## importing packages

# https://pandas.pydata.org/
import pandas as pd
# https://docs.python.org/2/library/glob.html
import glob 
# https://docs.python.org/2/library/os.html
import os 
# https://docs.python.org/2/library/gc.html
import gc 
# https://docs.scipy.org/doc/numpy/
import numpy as np
# https://docs.python.org/2/library/stringio.html
from StringIO import StringIO
# https://docs.python.org/2/library/time.html
import time

# initialize current time as t
t = time.time()

# ensure full output of numpy array is shown
np.set_printoptions(threshold=np.nan)

array_len = 1001 # size of each array

# dictionary to hold a summed [1x1001] numpy array for all motifs
motif_dict = dict()

######################################################################
# baf_folders is a list of all folders in folder that have ".baf" in the name

# method 1 + 2 are the same. 
# method 1 is more 'pythonic'

# method 1
baf_folders = [each for each in glob.glob('./folder/*') if '.baf' in each]
print 'baf_folders (method 1) = %s' % baf_folders

# method 2
baf_folders = []
for each in glob.glob('./folder/*'):
    if '.baf' in each:
        baf_folders.append(each)
print ''
print 'baf_folders (method 2) = %s' % baf_folders    
print ''

######################################################################

# looping through baf_folders list
for baf_folder in baf_folders:
    # garbage collection -- prevents script from crashing e.g. memory leaks
    gc.collect()
    # split string and take 3rd element (python is '0' based)
    baf_folder_name = baf_folder.split('/')[2]
    print ('baf_folders_name = %s' % (baf_folder_name))
    print ''
    # finding and looping through '.txt' files within baf_folders_name
    for each_file in glob.glob(baf_folder + '/*.txt' ):
        print('%s directory has the following text files: %s' % (baf_folder, each_file))
        print ''
        # making matrices folder within folder with bash command (os.system)
        bash_command = ''.join(['mkdir %s' % baf_folder, '/matrices'])
        print ('bash command = %s' % bash_command)
        bash_command_output = os.system(bash_command)
        if bash_command_output != 0:
            print('dir already exists')
        else:
            print('dir succeeded')
        print ('each_file = %s') % (each_file)
        print ''
        # read whole file in as a pandas dataframe
        data = pd.read_csv(each_file, sep = '\t', dtype={'motif_alt_id': str})

        # making a new column field called "label" that is the data
        # "\" -> line continuation
        data['label'] = baf_folder_name + '__' + data['# motif_id'].map(str) + \
        '__' + data['sequence_name'].map(str)
        
        # dynamic file path for storing each row's label and matrix
        matrix_filepath = ''.join([baf_folder,'/matrices/', \
                                   each_file.split('/')[-1][0:-4],'_matrix.csv'])
        # remove file initially if exists
        os.system('rm %s' % matrix_filepath)
         
        len_data = len(data)
        for counter, [start, stop, strand, motif, label] in enumerate(zip(data['start'], \
                                                                          data['stop'], \
                                                                          data['strand'], \
                                                                          data['# motif_id'],\
                                                                          data['label'])):
            
            # modify start and stop of 1s if strand is -
            if strand == '-': 
                start_orig = start 
                start = start = 1002 - stop
                stop = start + (stop - start_orig)
                
            # print progress every 50K rows of processing
            if counter % 50000 == 20:
                print 'progress ... %s/%s = %.2f pct' % (counter,len_data,\
                                                         100.0*float(counter)/float(len_data))
            # creating each row array based on start and stop variables
            l1, l2, l3 = start, stop-start, array_len-stop
            row_array1 = np.concatenate((np.repeat(0, l1),np.repeat(1, l2)), axis=None)
            row_array = np.concatenate((row_array1,np.repeat(0,l3)), axis = None)
            
            
            # outputing first 100 rows into csv files in created matrices folder
            if counter < 100:
                row_array_string = str(row_array.tolist()).replace('[','').replace(']','')
                row_array_string_w_label = label + ',' + row_array_string
                
                
                with open(matrix_filepath, 'a') as the_file:
                    the_file.write(row_array_string_w_label + '\n')
            
            # inserting motif keys and summed arrays (value) into dictionary motif_dict
            if motif not in motif_dict:
                motif_dict[str(motif)] = row_array
            else:
                # if motif key exists, add each binary array to existing array to get sum
                motif_dict[str(motif)] = np.add(motif_dict[str(motif)],row_array)
                
######################################################################

StringIO_str = ''
# iterate through dictionary of motifs and summed arrays
for k,v in motif_dict.iteritems():
    # print 'motif = %s' % k
    # print 'array value = %s' % v
    StringIO_str = StringIO_str + k + ',' + str(v).replace(']','').replace('[','') + '\n'
    
# final summed results by motif saved as csv in "./folder/motif_summed_result.csv"
motif_summed_result_dataframe = pd.read_csv(StringIO(StringIO_str))
motif_summed_result_dataframe.columns = ['motif', 'summed_array']
motif_summed_result_filepath = './folder/motif_summed_result.csv'
motif_summed_result_dataframe.to_csv(motif_summed_result_filepath,sep = ',')
print ''
print 'made it to the bitter end! in %.1f minutes ' % ((time.time() - t)/60.0)


''' testing output: 
baf_folders (method 1) = ['./folder/ttc1240.baf', './folder/vcap.baf', './folder/bt549.baf']

baf_folders (method 2) = ['./folder/ttc1240.baf', './folder/vcap.baf', './folder/bt549.baf']

baf_folders_name = ttc1240.baf

./folder/ttc1240.baf directory has the following text files: ./folder/ttc1240.baf/TTC1240_BRG1_500bp_Top10000.fimo.1e-100.motifs.txt

bash command = mkdir ./folder/ttc1240.baf/matrices
dir already exists
each_file = ./folder/ttc1240.baf/TTC1240_BRG1_500bp_Top10000.fimo.1e-100.motifs.txt

progress ... 20/1945518 = 0.00 pct
progress ... 50020/1945518 = 2.57 pct
progress ... 100020/1945518 = 5.14 pct
progress ... 150020/1945518 = 7.71 pct
progress ... 200020/1945518 = 10.28 pct
progress ... 250020/1945518 = 12.85 pct
progress ... 300020/1945518 = 15.42 pct
progress ... 350020/1945518 = 17.99 pct
progress ... 400020/1945518 = 20.56 pct
progress ... 450020/1945518 = 23.13 pct
progress ... 500020/1945518 = 25.70 pct
progress ... 550020/1945518 = 28.27 pct
progress ... 600020/1945518 = 30.84 pct
progress ... 650020/1945518 = 33.41 pct
progress ... 700020/1945518 = 35.98 pct
progress ... 750020/1945518 = 38.55 pct
progress ... 800020/1945518 = 41.12 pct
progress ... 850020/1945518 = 43.69 pct
progress ... 900020/1945518 = 46.26 pct
progress ... 950020/1945518 = 48.83 pct
progress ... 1000020/1945518 = 51.40 pct
progress ... 1050020/1945518 = 53.97 pct
progress ... 1100020/1945518 = 56.54 pct
progress ... 1150020/1945518 = 59.11 pct
progress ... 1200020/1945518 = 61.68 pct
progress ... 1250020/1945518 = 64.25 pct
progress ... 1300020/1945518 = 66.82 pct
progress ... 1350020/1945518 = 69.39 pct
progress ... 1400020/1945518 = 71.96 pct
progress ... 1450020/1945518 = 74.53 pct
progress ... 1500020/1945518 = 77.10 pct
progress ... 1550020/1945518 = 79.67 pct
progress ... 1600020/1945518 = 82.24 pct
progress ... 1650020/1945518 = 84.81 pct
progress ... 1700020/1945518 = 87.38 pct
progress ... 1750020/1945518 = 89.95 pct
progress ... 1800020/1945518 = 92.52 pct
progress ... 1850020/1945518 = 95.09 pct
progress ... 1900020/1945518 = 97.66 pct
baf_folders_name = vcap.baf

./folder/vcap.baf directory has the following text files: ./folder/vcap.baf/Vcap_BAF155_500bp_Top10000.fimo.1e-100.motifs.txt

bash command = mkdir ./folder/vcap.baf/matrices
dir already exists
each_file = ./folder/vcap.baf/Vcap_BAF155_500bp_Top10000.fimo.1e-100.motifs.txt

progress ... 20/1727458 = 0.00 pct
progress ... 50020/1727458 = 2.90 pct
progress ... 100020/1727458 = 5.79 pct
progress ... 150020/1727458 = 8.68 pct
progress ... 200020/1727458 = 11.58 pct
progress ... 250020/1727458 = 14.47 pct
progress ... 300020/1727458 = 17.37 pct
progress ... 350020/1727458 = 20.26 pct
progress ... 400020/1727458 = 23.16 pct
progress ... 450020/1727458 = 26.05 pct
progress ... 500020/1727458 = 28.95 pct
progress ... 550020/1727458 = 31.84 pct
progress ... 600020/1727458 = 34.73 pct
progress ... 650020/1727458 = 37.63 pct
progress ... 700020/1727458 = 40.52 pct
progress ... 750020/1727458 = 43.42 pct
progress ... 800020/1727458 = 46.31 pct
progress ... 850020/1727458 = 49.21 pct
progress ... 900020/1727458 = 52.10 pct
progress ... 950020/1727458 = 55.00 pct
progress ... 1000020/1727458 = 57.89 pct
progress ... 1050020/1727458 = 60.78 pct
progress ... 1100020/1727458 = 63.68 pct
progress ... 1150020/1727458 = 66.57 pct
progress ... 1200020/1727458 = 69.47 pct
progress ... 1250020/1727458 = 72.36 pct
progress ... 1300020/1727458 = 75.26 pct
progress ... 1350020/1727458 = 78.15 pct
progress ... 1400020/1727458 = 81.05 pct
progress ... 1450020/1727458 = 83.94 pct
progress ... 1500020/1727458 = 86.83 pct
progress ... 1550020/1727458 = 89.73 pct
progress ... 1600020/1727458 = 92.62 pct
progress ... 1650020/1727458 = 95.52 pct
progress ... 1700020/1727458 = 98.41 pct
baf_folders_name = bt549.baf

./folder/bt549.baf directory has the following text files: ./folder/bt549.baf/BT549_BRG1_500bp_Top10000.fimo.1e-100.motifs.txt

bash command = mkdir ./folder/bt549.baf/matrices
dir already exists
each_file = ./folder/bt549.baf/BT549_BRG1_500bp_Top10000.fimo.1e-100.motifs.txt

progress ... 20/1765148 = 0.00 pct
progress ... 50020/1765148 = 2.83 pct
progress ... 100020/1765148 = 5.67 pct
progress ... 150020/1765148 = 8.50 pct
progress ... 200020/1765148 = 11.33 pct
progress ... 250020/1765148 = 14.16 pct
progress ... 300020/1765148 = 17.00 pct
progress ... 350020/1765148 = 19.83 pct
progress ... 400020/1765148 = 22.66 pct
progress ... 450020/1765148 = 25.49 pct
progress ... 500020/1765148 = 28.33 pct
progress ... 550020/1765148 = 31.16 pct
progress ... 600020/1765148 = 33.99 pct
progress ... 650020/1765148 = 36.83 pct
progress ... 700020/1765148 = 39.66 pct
progress ... 750020/1765148 = 42.49 pct
progress ... 800020/1765148 = 45.32 pct
progress ... 850020/1765148 = 48.16 pct
progress ... 900020/1765148 = 50.99 pct
progress ... 950020/1765148 = 53.82 pct
progress ... 1000020/1765148 = 56.65 pct
progress ... 1050020/1765148 = 59.49 pct
progress ... 1100020/1765148 = 62.32 pct
progress ... 1150020/1765148 = 65.15 pct
progress ... 1200020/1765148 = 67.98 pct
progress ... 1250020/1765148 = 70.82 pct
progress ... 1300020/1765148 = 73.65 pct
progress ... 1350020/1765148 = 76.48 pct
progress ... 1400020/1765148 = 79.31 pct
progress ... 1450020/1765148 = 82.15 pct
progress ... 1500020/1765148 = 84.98 pct
progress ... 1550020/1765148 = 87.81 pct
progress ... 1600020/1765148 = 90.65 pct
progress ... 1650020/1765148 = 93.48 pct
progress ... 1700020/1765148 = 96.31 pct
progress ... 1750020/1765148 = 99.14 pct

made it to the bitter end! in 3.7 minutes 
'''
