## For my own reference, here is how to easily download an HTTP file and 
## upload it to a specific S3 path.

## This library should be built-into Python3 and allow you to download a file
import urllib.request
urllib.request.urlretrieve ("https://www.guru99.com/images/Pythonnew/python19_1.png", "my_pic.png")

## And now upload the pic to my specific path
file_name = "my_pic.png"
bucket_name = "my-bucket"
upload_location = "path/to/my/file/my_pic.png"
s3_resource.Bucket(bucket_name).upload_file(file_name,upload_location)

## Done! Check S3 for your new file. 
