from __future__ import print_function

import os, sys, re, json, requests, boto3
from utils import submit_job

print ('Loading function')


def lambda_handler(event, context):
    '''
    This lambda handler calls submit_job with the job type info
    and product id from the sns message
    '''

    print("Got event of type: %s" % type(event))
    print("Got event: %s" % json.dumps(event))
    print("Got context: %s"% context)
    
    # parse sns message
    message = json.loads(event["Records"][0]["Sns"]["Message"])
    
    # parse s3 event
    s3_info = message['Records'][0]['s3']
    
    # parse met and dataset files and urls
    bucket = s3_info['bucket']['name']
    met_file = s3_info['object']['key']
    ds_file = met_file.replace('.met.json', '')
    ds_url = "s3://%s/%s/%s" % (os.environ['DATASET_S3_ENDPOINT'], bucket, ds_file)
    
    # read in metadata
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, met_file)
    md = json.loads(obj.get()['Body'].read())
    print("Got metadata: %s" % json.dumps(md, indent=2))
    
    # dataset id
    id = md['id']
    
    #submit mozart jobs to update ES
    job_type = os.environ['JOB_TYPE'] # e.g. "INGEST_L0A_LR_RAW"
    job_release = os.environ['JOB_RELEASE'] # e.g. "gman-dev"
    job_spec = "job-%s:%s" % (job_type, job_release)
    job_params = {
        "id": id,
        "raw_url": ds_url,
        "raw_file": os.path.basename(ds_url),
        "prod_met": md,
    }
    queue = os.environ['JOB_QUEUE'] # eg.g "factotum-job_worker-large"
    tags = ["data-staged"]

    # submit mozart job
    submit_job(job_spec, job_params, queue, tags)
