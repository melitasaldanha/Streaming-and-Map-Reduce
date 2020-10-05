##########################################################################
## Simulator.py  v0.2
##
## Implements two versions of a multi-level sampler:
##
## 1) Traditional 3 step process
## 2) Streaming process using hashing
##
## Original Code written by H. Andrew Schwartz
## for SBU's Big Data Analytics Course 
## Spring 2020
##
## Student Name: Melita Saldanha
## Student ID: 112551230

##Data Science Imports: 
import numpy as np
import mmh3
from random import random
from random import shuffle

##IO, Process Imports: 
import sys
from pprint import pprint
import datetime


##########################################################################
##########################################################################
# Task 1.A Typical non-streaming multi-level sampler

def typicalSampler(filename, percent = .01, sample_col = 0):
    # Implements the standard non-streaming sampling method
    #   filename -- name of file to run the traditional algorithm over
    #   percent -- percent of sample to keep
    #   sample_col -- column number to sample over
    #   (assume the last column is always the value to compute mean)
    #
    # Step 1: read file to pull out unique user_ids from file
    # Step 2: subset to random percent of user_ids
    # Step 3: read file again to pull out records from the 1% user_id and compute mean withdrawn

    mean, standard_deviation = 0.0, 0.0

    ##<<COMPLETE>>

    # Read file and get a set of unique user ids from the file
    with open(filename, 'r') as input:
      unique_user_ids = list(set([int(line.split(",")[sample_col]) for line in input]))

    # Shuffle the user ids obtained to randomize the sample
    shuffle(unique_user_ids)

    # Find required percentage of user ids from the list
    len_random_users = len(unique_user_ids)*percent
    random_unique_user_ids = unique_user_ids[:int(len_random_users)]

    # List to store user ids from the file that match
    result_last_col = []

    # Traverse file again to match rows with user id list generated
    with open(filename, 'r') as input:
      for line in input:
        curr_line = line.split(",")
        user_id = int(curr_line[sample_col])

        # If user id of current row exists in generated list, append value of last column to result
        if user_id in random_unique_user_ids:
          result_last_col.append(float(curr_line[-1].strip("\n")))

    # Find mean and standard deviation of obtained last column values
    mean = np.mean(result_last_col)
    standard_deviation = np.std(result_last_col)


    return mean, standard_deviation


##########################################################################
##########################################################################
# Task 1.B Streaming multi-level sampler

def streamSampler(stream, percent = .01, sample_col = 0):
    # Implements the standard streaming sampling method:
    #   stream -- iosteam object (i.e. an open file for reading)
    #   percent -- percent of sample to keep
    #   sample_col -- column number to sample over
    #
    # Rules:
    #   1) No saving rows, or user_ids outside the scope of the while loop.
    #   2) No other loops besides the while listed. 
    
    mean, standard_deviation = 0.0, 0.0

    ##<<COMPLETE>>

    # Obtain total number of buckets and required number of buckets for hash function
    decimal_value = (str(percent).split('.'))[1]
    total_num_of_buckets = 10**len(decimal_value)
    reqd_num_of_buckets = int(decimal_value)

    # Store sum and length of list while streaming for last column values
    sum = 0
    sum_of_sq = 0
    length = 0

    for line in stream:
        ##<<COMPLETE>>
        line_values = line.split(",")
        user_id = line_values[sample_col]

        # If hash value of user id falls into the required buckets, value of last column is considered
        if(hash(user_id)%total_num_of_buckets < reqd_num_of_buckets):

          # Value of last column for current line
          last_col_val = float(line_values[-1])

          sum += last_col_val
          sum_of_sq += last_col_val**2
          length = length+1
        
    ##<<COMPLETE>>

    # Compute mean and standard deviation
    mean = sum/length
    standard_deviation = np.sqrt((sum_of_sq/length)-(mean**2))

    return mean, standard_deviation


##########################################################################
##########################################################################
# Task 1.C Timing

files=['transactions_small.csv', 'transactions_medium.csv', 'transactions_large.csv']
percents=[.02, .005]

if __name__ == "__main__": 

    ##<<COMPLETE: EDIT AND ADD TO IT>>
    for perc in percents:
        print("\nPercentage: %.4f\n==================" % perc)
        for f in files:
            print("\nFile: ", f)
            
            start_time = datetime.datetime.now()
            print("  Typical Sampler: ", typicalSampler(f, perc, 2))
            end_time = datetime.datetime.now()
            print("Time Taken: ", (((end_time-start_time).total_seconds())*1000), "milliseconds \n")

            start_time = datetime.datetime.now()
            fstream = open(f, "r")
            print("  Stream Sampler:  ", streamSampler(fstream, perc, 2))
            end_time = datetime.datetime.now()
            print("Time Taken: ", (((end_time-start_time).total_seconds())*1000), "milliseconds \n")