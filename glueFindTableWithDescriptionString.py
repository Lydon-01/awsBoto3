## This script takes a string "mySearch" and returns Glue table in the given Database that contain that string. 
## Note that this is a first-draft script and not rounded off yet. Lots can be improved. For example, the
## mySearch string must match exactly to be find, so if there is a full stop after the word in the 
## description, it must be provided in mySearch. 

# Libraries and clients 
import boto3
client = boto3.client("glue")

# What phrase in the description are you looking for?
mySearch = "test."

# Choose which database to search: 
myDatabase = "csv"

# Create an array of all the table names
count = 0
tablesnames = []
tablelist = client.get_tables(DatabaseName = myDatabase)
for table in tablelist["TableList"]:
#    print(tablelist["TableList"][count]["Name"])
    tablesnames.append(tablelist["TableList"][count]["Name"])
#    print(tablesnames)
    count+=1

# Use the tablenames array to check if a table has a description 
# then save the table name and description to a new array. 
descr_found = []
for table in tablesnames:
    tableinfo = client.get_table(DatabaseName = "csv", Name = table)
    #print(tableinfo["Table"]["Name"])
    if "Description" in tableinfo["Table"]:
        #print(tableinfo["Table"]["Description"])
        descr_found .append([tableinfo["Table"]["Name"],tableinfo["Table"]["Description"]])
        #print(descr_found)

# Split the description into words and search for the substring of mySearch. If it finds mySearch in the description words, print the table name and description. 
position = 0
while position < len(descr_found):
	words = ""
	words = descr_found[position][1].split()
	if mySearch in words: #see if one of the words in the sentence is the word we want
		print(descr_found[0])
	position +=1

# Libraries and clients 
import boto3
client = boto3.client("glue")

# What phrase in the description are you looking for?
mySearch = "test."

# Choose which database to search: 
myDatabase = "csv"

# Create an array of all the table names
count = 0
tablesnames = []
tablelist = client.get_tables(DatabaseName = myDatabase)
for table in tablelist["TableList"]:
#    print(tablelist["TableList"][count]["Name"])
    tablesnames.append(tablelist["TableList"][count]["Name"])
#    print(tablesnames)
    count+=1


# Use the tablenames array to check if a table has a description 
# then save the table name and description to a new array. 
descr_found = []
for table in tablesnames:
    tableinfo = client.get_table(DatabaseName = "csv", Name = table)
    #print(tableinfo["Table"]["Name"])
    if "Description" in tableinfo["Table"]:
        #print(tableinfo["Table"]["Description"])
        descr_found .append([tableinfo["Table"]["Name"],tableinfo["Table"]["Description"]])
        #print(descr_found)

# Split the description into words and search for the substring of mySearch. If it finds mySearch in the description words, print the table name and description. 
position = 0
while position < len(descr_found):
	words = ""
	words = descr_found[position][1].split()
	if mySearch in words: #see if one of the words in the sentence is the word we want
		print(descr_found[0])
	position +=1
	
	
## OPTIONAL 
# Write the results to a csv file that can be used for a Glue Table via S3: 
import csv
with open('desc.csv', mode='w') as desc:
	desc_writer = csv.writer(desc, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
	for i in descr_found:
		desc_writer.writerow(i)
