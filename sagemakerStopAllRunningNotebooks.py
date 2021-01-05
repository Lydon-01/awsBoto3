## This Python script will stop all the running Sagemaker notebooks for the region you are in. 
## This will also stop Notebooks connected to Glue Dev Endpoints 
## Related link: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.stop_notebook_instance

## Stop all running Sagemaker Notebooks in current region 
sm = boto3.client("sagemaker")
notebook_list = sm.list_notebook_instances()
for notebook in notebook_list["NotebookInstances"]:
	if notebook["NotebookInstanceStatus"] == "InService":
		sm.stop_notebook_instance(NotebookInstanceName=notebook["NotebookInstanceName"])
		print("Stopped InService Sagemaker notebook: ", notebook["NotebookInstanceName"])
