from __future__ import print_function

import os, sys, re, json, requests, boto3
from utils import submit_job

print ('Loading function')

signal_file_suffix = None

if "SIGNAL_FILE_SUFFIX" in os.environ:
    signal_file_suffix = os.environ['SIGNAL_FILE_SUFFIX']


def __get_job_type_info(data_file, job_types, default_type, default_release,
                   default_queue):
    """
    Determine the job type.

    :param data_file: The data file being ingested.
    :param job_types: A mapping of job types to a regex.
    :param default_type: Default job type.
    :param default_release: Default job release version.
    :param: default_queue: Default job queue.
    :return: Function will try to match the given data file to one of 
    the types specified in the job type mapping. If no mapping exists 
    or a match could not be found, then it will use the default job
    type, release, and queue.
    """
    for type in job_types.keys():
        regex = job_types[type]['PATTERN']
        print("Checking if {} matches {}".format(regex, data_file))
        match = re.search(regex, data_file)
        if match:
            release = job_types[type]['RELEASE']
            queue = job_types[type]['QUEUE']
            print("Data file '{}' matches job type info: "
                  "type: {}, release: {}, queue: {}".format(
                data_file, type, release, queue))
            return type, release, queue
    print("Could not match data file '{}' to a given job type: {}. "
          "Using default job type info".format(data_file, job_types.keys()))
    return default_type, default_release, default_queue


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
    
    # parse signal and dataset files and urls
    bucket = s3_info['bucket']['name']
    trigger_file = s3_info['object']['key']
    print("Trigger file: {}".format(trigger_file))
    if signal_file_suffix:
        ds_file = trigger_file.replace(signal_file_suffix, '')
    else:
        ds_file = trigger_file
    ds_url = "s3://%s/%s/%s" % (os.environ['DATASET_S3_ENDPOINT'], bucket,
                                ds_file)
    
    # read in metadata
    md = {}
    if signal_file_suffix:
        s3 = boto3.resource('s3')
        obj = s3.Object(bucket, trigger_file)
        md = json.loads(obj.get()['Body'].read())
        print("Got signal metadata: %s" % json.dumps(md, indent=2))

    # data file
    id = data_file = os.path.basename(ds_url)
    
    # submit mozart jobs to update ES
    default_job_type = os.environ['JOB_TYPE'] # e.g. "INGEST_L0A_LR_RAW"
    default_job_release = os.environ['JOB_RELEASE'] # e.g. "gman-dev"
    default_queue = os.environ['JOB_QUEUE']
    job_types = {}
    if 'JOB_TYPES' in os.environ:
        job_types = json.loads(os.environ['JOB_TYPES'])
    job_type, job_release, queue = __get_job_type_info(data_file, job_types,
                                                       default_job_type,
                                                       default_job_release,
                                                       default_queue)
    job_spec = "job-%s:%s" % (job_type, job_release)
    job_params = {
        "id": id,
        "data_url": ds_url,
        "data_file": data_file,
        "prod_met": md,
    }
    tags = ["data-staged"]

    # submit mozart job
    submit_job(job_spec, job_params, queue, tags)
