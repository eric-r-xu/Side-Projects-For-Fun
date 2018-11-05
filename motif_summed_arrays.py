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
