## This function should help you to automatically trigger the run of newly created crawler. 
## Work in progress, test and use discression 
# 
# 1. Open the Lambda console and choose Create Function
#    1.1. Function name "glueStartLastCreatedCrawler"
#    1.2. Runtime: Python 2.7
#    1.3. Choose your existing Role
#    1.4. Create function
# 2. Delete all existing code and paste the following. Be sure to replace your SNS Target ARN!
###########################################################################
import boto3
client = boto3.client('glue')

# Create a list of crawlers and creation dates
def lambda_handler(event, context):
    crawlers_raw=client.get_crawlers()
    c_list=[]
    for i in range(len(crawlers_raw['Crawlers'])):
        c_name = crawlers_raw['Crawlers'][i]['Name']
        c_creation = crawlers_raw['Crawlers'][i]['CreationTime']
        c_list.append([c_name,c_creation])
    
        # Find latest created
        c_highest = c_list[0]
        for i in range(len(c_list)):
            c_latest = c_list[i]
            if c_latest[1] > c_highest[1]:
                c_highest = c_latest
    print c_highest
            
###########################################################################
