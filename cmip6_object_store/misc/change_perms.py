import json
import boto3
import botocore

from cmip6_object_store.config import CONFIG
from cmip6_object_store.cmip6_zarr.utils import get_zarr_path, get_credentials


project = 'cmip6'

"""
Changes permissions on all buckets, and writes output files "successes" and
"failures" with relevant lists. Note that (deliberately), "failures" only
apply to "Access Denied" - a failure for any other reason will abort this
script.
"""

class PolicyChanger:

    def __init__(self):
        creds = get_credentials()

        s3_uri = CONFIG["store"]["endpoint_url"]

        self._clnt = boto3.client('s3', endpoint_url=s3_uri,    
                                  aws_access_key_id=creds['token'],
                                  aws_secret_access_key=creds['secret'])

        policy = {
            "Version": "2008-10-17",
            "Id": "Unnamed Policy",
            "Statement": [
                {
                    "Sid": "Read-only access for Everyone",
                    "Effect": "Allow",
                    "Principal": {
                        "anonymous": [
                            "*"
                        ]
                    },
                    "Action": [
                        "GetObject"
                    ],
                    "Resource": "*"
                }
            ]
        }

        self._bucket_policy_s = json.dumps(policy)

        
    def change_bucket_policy(self, bucket):

        self._clnt.put_bucket_policy(Bucket=bucket, Policy=self._bucket_policy_s)


def get_buckets():

    datasets_file = CONFIG["datasets"]["datasets_file"]

    buckets = set()
    with open(datasets_file) as f:
        for line in f:
            dataset_id = line.strip().split(',')[0]
            bucket, obj_name = get_zarr_path(dataset_id, project)
            buckets.add(bucket)

    return sorted(buckets)


def main():

    changer = PolicyChanger()
    buckets = get_buckets()
    n = len(buckets)
    
    with open('successes', 'w') as succlog, open('failures', 'w') as faillog:
        for i, bucket in enumerate(buckets):
            print(f'{i}/{n} {bucket} ', end='')
            try:
                changer.change_bucket_policy(bucket)
            except botocore.exceptions.ClientError as exc:
                if 'Access Denied' in str(exc):
                    print('FAIL')
                    faillog.write(f'{bucket}\n')
                else:
                    raise
            else:
                print('Succ')
                succlog.write(f'{bucket}\n')    

main()
