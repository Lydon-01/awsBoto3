## I spent a long time struggling to find the right format for submitting a PySpark step with additional libraries. 
## This is here to help others avoid future struggles and for me to find when I need it.
##
## Prerequisits:
## 1. Create and upload your Python Library file to s3. Example: 
## (lib.py)
## def run():
##   print('Happy Birthday!')
##
## 2. Create and upload your PySpark script to s3. Example: 
## (script.py)
## from pyspark import SparkContext, SparkConf
## from pyspark.sql import SparkSession
## sc = SparkContext()
## import hbd
## hbd.run()
## 
## 3. You will now build your boto3 code that will launch an EMR step and execute Pyspark code. 
##    Replace the IAM keys, the cluster ID and the S3 paths
##    The command works successfully when the script is at the end and the command looks as follows: 
## ===================================================================================================
import boto3

step_args = ['/usr/bin/spark-submit', '--master', 'yarn', '--py-files', 's3://lydon/temp/lib.zip', '--deploy-mode', 'cluster', 's3://lydon/temp/script.py']
cluster_id = 'j-ABCxxxx'
aws_key_id = 'AKIARDVxxxxxxxxxUI'
aws_secret = '8dpWCxxxxxxxxxxxxxxxxxxxxxxxEp'

conn = boto3.client('emr', region_name='us-east-1', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_secret)
step = {
  'Name': 'lytest',
  'ActionOnFailure': 'CONTINUE',
  'HadoopJarStep': {
	'Jar': '/mnt/var/lib/hadoop/steps/s-2VWE5UNU5VFK3/script-runner.jar',
	'Args': step_args,
	}
  }
action = conn.add_job_flow_steps(JobFlowId = cluster_id, Steps = [step])
print(action)
## ====================================================================================================

## 4. Note that The additional libraries (py-files) can be specified as a .zip, a whole directory (s3://folder/) or as the .py. All three work. 
## --py-files s3://lydon/temp/lib.zip
## --py-files s3://lydon/temp/
## --py-files s3://lydon/temp/lib.py

## 5. What helped me a lot in uncovering this behaviour was to login to the Task Node an check the logs under the container: 
##        cd /mnt/var/log/hadoop-yarn/containers
##        cat application_1578313644403_0023/container_1578313644403_0023_01_000001/stderr

## 5. Interestingly, I also found my tests worked when copying the script file locally, then executing the command with the local script: 
## [hadoop@ip-172-31-18-123 ~]$ aws s3 cp s3://lydon/temp/script.py .
## [hadoop@ip-172-31-18-123 ~]$ hadoop jar /mnt/var/lib/hadoop/steps/s-2VWE5UNU5VFK3/script-runner.jar /usr/bin/spark-submit script.py --py-files s3://lydon-glue/temp/hbd.py --deploy-mode cluster 
