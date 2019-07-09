import sys
import boto3
import json
import time
import argparse


parser = argparse.ArgumentParser(description='Publish existing bucket objects as message to SNS topic.')
parser.add_argument('region', type=str, metavar='', help='Region of your AWS account.')
parser.add_argument('profile', type=str, metavar='', help='Name of the AWS profile to use.')
parser.add_argument('bucket', type=str, metavar='', help='Name of the bucket storing objects to be processed.')
parser.add_argument('topic_arn', type=str, metavar='', help='ARN of the topic to publish to.')
parser.add_argument('-d', '--delay', type=float, default=0.10, metavar='',
                    help='Time buffer between message publishings.')
args = parser.parse_args()

def publish_messages(contents):
    global cnt, unsuccessful_msg_keys
    for content in contents:
        key = content['Key']
        image_id = key.split('/')[-1].split('.')[0]
        if any([key.endswith(ending) for ending in ENDINGS]):
            message = {
                        "Records": [
                            {
                                "s3": {
                                    "bucket": {
                                        "name": BUCKET
                                    },
                                    "object": {
                                        "key": key
                                    }
                                }
                            }
                        ]
                    }
                    
            sns_response = sns_client.publish(
              TopicArn=TOPIC_ARN,
              Message=json.dumps(message)
            )
                        
            if sns_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print('\rMessage #{} - "{}" - published to SNS topic.'.format(cnt, key), end='')
                sys.stdout.flush()
                cnt += 1
            else:
                unsuccessful_msg_keys.append(key)
            # include a time buffer to prevent pipeline overload
            time.sleep(DELAY)


if __name__ == '__main__':
    REGION = args.region
    PROFILE = args.profile
    BUCKET = args.bucket
    TOPIC_ARN = args.topic_arn
    DELAY = args.delay
    assert(DELAY >= 0.0), 'Time delay value must be numeric value >= 0.0'
    # the endings of file names to keep
    ENDINGS = ['flt.fits',
               'flc.fits']
    
    sess = boto3.Session(region_name=REGION,
                         profile_name=PROFILE)
    s3_client = sess.client('s3')
    sns_client = sess.client('sns')
    step_client = sess.client('stepfunctions')
    s3_response = s3_client.list_objects_v2(Bucket=BUCKET,
                                            RequestPayer='requester')


    cnt = 1
    unsuccessful_msg_keys = []
    # run through all objects in the bucket, then SNS topic subscription will take over
    while 'NextContinuationToken' in s3_response.keys():
        # responses come in batches of 1,000 or less
        token = s3_response['NextContinuationToken']
        publish_messages(s3_response['Contents'])
        # get a new batch of 1,000 responses
        s3_response = s3_client.list_objects_v2(Bucket=BUCKET,
                                                RequestPayer='requester',
                                                ContinuationToken=token)
    publish_messages(s3_response['Contents'])
    msg = '\nThere were {} unsuccessfully published messages.\n'.format(len(unsuccessful_msg_keys))
    print(msg + '=' * (len(msg)-2) + '\nKEYS:')
    if len(unsuccessful_msg_keys):
        for key in unsuccessful_msg_keys:
            print(key)
