# The function should take two arguments:
bucket_name =  "mybucket" # the name of the S3 bucket to search
prefix= "p_day"  # a prefix string to filter the S3 objects

# The function should return a list of all S3 objects that match the search criteria.

import boto3
s3 = boto3.resource('s3')

def count_s3_objects(bucket_name, prefix):
    bucket = s3.Bucket(bucket_name)
    objects = [o for o in bucket.objects.filter(Prefix=prefix)]
    return objects

print(count_s3_objects(bucket_name, prefix))

