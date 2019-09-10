#!/usr/bin/python3
# by Lydon Carter, for AWS, September 2019 

#########################################################################
# This script will create "folders" (S3 keys) in a Hive-Style partition  
# naming-format, in a loop. Then, within each partition, it creates 
# several sub-partitions. 
# Assume you would like your structure to look as follows: 
# s3://best-bucket/y=x/m=x/file_x.yz
##########################################################################
## PERSONAL FUTURE USEFUL 
## print ("Please give me a number: ")
## number = raw_input()

# Context
import boto3 
s3 = boto3.client('s3')

# You must change these
bucket_name = "YOUR-BUCKET-NAME"  #Just the bucket name. Example "best-bucket"
bucket_name = "lydon-glue"
bucket_dir = "YOUR/SUB/DIRECTORY/" #Where do you want these folders created?
bucket_dir = "datasets/csv/csv_gz_partitioned/"
top_part = "y=" #The top-level partition you are creating. Example "y="
sub1_part = "d=" #The sub-partition in side each top-level partition. Example "d="

# Define primitives for loop
max_top_part = "100" #How many top-level partitions do you want? Example 1000 
max_sub1_part = "10" #How many sub-partitions do you want in each top partition? Example 365

# Loop fun starts here. Create partitions for the number of 
# top-partition you defined, AND create sub-partitions inside each.
for top in range(int(max_top_part)):
	for sub in range(int(max_sub1_part)):
		s3.put_object(Bucket=bucket_name, Key=(bucket_dir+top_part+str(top)+'/'+sub1_part+str(sub)+'/'))
		
