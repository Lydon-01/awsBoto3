# The function should take two arguments:
bucket_name = "mybucket" # the name of the S3 bucket to search
prefix= "myString"  # a string to find in the S3 objects

# The function will count all S3 objects that match the search criteria.
import boto3
s3 = boto3.resource('s3')

bucket = s3.Bucket(bucket_name)
count = 0
for obj in bucket.objects.all():
   if prefix in obj.key:
       count += 1
print(count)
