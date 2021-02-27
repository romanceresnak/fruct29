from urllib.parse import unquote_plus
import boto3
import time

sm = boto3.client("sagemaker")

def lambda_handler(event, context):
	bucket = event["Records"][0]["s3"]["bucket"]["name"]
	key = unquote_plus(event["Records"][0]["s3"]["object"]["key"])
	input_loc = "s3://" + bucket + "/" + key
	
	batch_job_name = key[key.rindex('/')+1:].replace(".csv", "")+'-hotelpred-'+time.strftime("%Y%m%d%H%M")
	
	request = {
	    "TransformJobName": batch_job_name,
	    "ModelName": 'hotel-cancellation-sklearn-rf',
		...
	    "TransformOutput": {
	        "S3OutputPath": 's3://snowflake2sagemaker-prediction-out/sagemaker/',
		    ...
	    },
	    "TransformInput": {
	        "DataSource": {
	            "S3DataSource": {
	                "S3DataType": "S3Prefix",
	                "S3Uri": input_loc 
	            }
	        },
		    ...
	    },
	    "TransformResources": {
		    ...
	    }
	}
	sm.create_transform_job(**request)