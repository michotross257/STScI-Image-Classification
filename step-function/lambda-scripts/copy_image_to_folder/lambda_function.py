import os
import json
import boto3


# environment variables
destination_bucket_name = os.environ.get('DESTINATION_BUCKET')

# acquire AWS service access
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


def lambda_handler(event, context, call=None, callback=None):
    bucket = event['s3']['bucket']
    key = event['s3']['key']
    
    # copy image to destination bucket to corresponding folder based on the predicted class
    destination_bucket = s3_resource.Bucket(destination_bucket_name)
    copy_source = {'Bucket': destination_bucket, 'Key': key}
    destination_bucket.copy(
        copy_source,
        event['classification']['predicted_class'] + '/' + event['image_id']
    )
    
    return event
