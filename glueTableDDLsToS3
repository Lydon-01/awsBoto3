## glueTableDDLsToS3.py - by Lydon Carter - May 2019
## This script will loop through all databases and tables in your Glue Data Catalog for a region
## and run an Athena SHOW TABLE to backup their DDL's to S3.

# Set your variables
region_var = 'us-east-1'  # Change the region for the script to run
s3_var = 's3://my-glue/ddl_backups/'  # Change the S3 location where the query will write DDL files

# Modules and basics
import boto3
client = boto3.client('glue',region_name=region_var)  

# Create a list of all the Glue Databases
responseGetDatabases = client.get_databases()
databaseList = responseGetDatabases['DatabaseList']

# Definite a function for starting an Athena query
def run_query(query, database, s3_output):
	client = boto3.client('athena')
	response = client.start_query_execution(
		QueryString=query,
		QueryExecutionContext={
			'Database': database
			},
		ResultConfiguration={
			'OutputLocation': s3_output,
			}
		)
	print('Completed '+s3_output+' with Execution ID: ' + response['QueryExecutionId'])

# Create a list for all the Glue Tables
for databaseDict in databaseList:
	databaseName = databaseDict['Name']
	#print('databaseName: ' + databaseName)  # Optionally you can print the names of the tables
	responseGetTables = client.get_tables( DatabaseName = databaseName )
	tableList = responseGetTables['TableList']
	
	# Loop through the tableList and write out the table DDL to respective directory in S3 using the run_query function
	for tableDict in tableList:
		try:
			print('')
			tableName = tableDict['Name']
			#print('-- tableName: '+tableName)  # Optionally you can print out the names of the tables
			run_query("show create table "+tableName, databaseName, s3_var+databaseName+"/"+tableName)
		except:
			print("There was an error with "+tableName+" in database "+databaseName)
