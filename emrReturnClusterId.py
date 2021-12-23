# On an EMR Cluster, this Python script will return its own Cluster ID. 

def get_cluster_id():
	with open('/mnt/var/lib/info/job-flow.json') as jobflow:
		jobflowdata = jobflow.read()
		jobflowjson = json.loads(jobflowdata)
		return jobflowjson['jobFlowId']

get_cluster_id()
