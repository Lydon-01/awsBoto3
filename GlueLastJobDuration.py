## Python 2.7 
## GlueLastRunDuration.py 
## Version 1
## by Lydon Carter October 2018

## USE
# Script to get a specific AWS Glue Job and tell you the duration of
# the last run.
# Notes:
# -- The script will use the location you setup for your Glue Context in the "Needed stuff"
# -- This is basically a Beta. Very simply and you can easily break it.

## CODE 
# Needed stuff
import boto3
import sys
glue = boto3.client(service_name='glue', region_name='us-east-1',endpoint_url='https://glue.us-east-1.amazonaws.com')

# Ask they user what they want
print ""
print "This script will check the duration of the last run of a job."
print "Enter the job name OR type \"show\" to see the list of your jobs."
prompt = raw_input("") # The user doesn't need to enter quotes, they can simply enter the name.
print ""
if prompt == 'show':
	# If they want to list their jobs, create a dictionary of jobs and list them. 
	jobsDic = glue.get_jobs()
	print "-" * 70 
	for i in jobsDic['Jobs']:
		print i['Name']
	print "-" * 70 
	print ""
	print "Enter the job name to check its last run duration."
	prompt = raw_input("")
print ""

# Now, get the runs of the prompted name and save the last Id
print "Duration for Glue Job \"" + prompt + "\":"
allRuns = glue.get_job_runs(JobName=prompt)
allRunsIds = []
for i in allRuns["JobRuns"]:
	#print i['Id']
	allRunsIds.append(i['Id'])

lastRunId = allRunsIds[-1]

# Get the duration of the last run and show useful info
status = glue.get_job_run(JobName=prompt, RunId=lastRunId)
print "-" * 70 
print "Last JobRun ID: " + str(lastRunId)
print "JobRunState:    " + str(status['JobRun']['JobRunState'])
print "StartedOn:      " + str(status['JobRun']['StartedOn'])
print "CompletedOn:    " + str(status['JobRun']['CompletedOn'])
print "-" * 32 + " DONE " + "-" * 32
print ""
print ""