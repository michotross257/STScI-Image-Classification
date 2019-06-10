import os
import json
import uuid
import boto3
import pytz
from decimal import Decimal
from datetime import datetime


# --------------------------- EDIT THESE VALUES ---------------------------
time_zone = '' # for a list of times: print(pytz.all_timezones)
classes   = [] # e.g. ['CLUSTER', 'DEEP', 'NEBULA', 'STARS']
# --------------------------- EDIT THESE VALUES ---------------------------


endpoint_name = os.environ['ENDPOINT_NAME']
table_id = os.environ.get('IMAGE_CLASSIFICATIONS_TABLE')
destination_bucket_name = os.environ.get('DESTINATION_BUCKET')
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
runtime = boto3.client('runtime.sagemaker')
db_resource = boto3.resource('dynamodb')
db_table = db_resource.Table(table_id)


def lambda_handler(event, context):
    '''For each new image added to source bucket:
            1) pass to SageMaker endpoint for classification
            2) update dynamodb table with classification info
            3) copy image to appropriate folder in destination bucket
            4) delete original image from source bucket
            5) log classification info
       Function returns None.'''
    for index, record in enumerate(event['Records']):
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        download_path = '/tmp/{}'.format(key)
        # download image and send off to automl for classification
        s3_client.download_file(bucket, key, download_path)
        with open(download_path, 'rb') as downloaded_file:
            content = downloaded_file.read()
        response = runtime.invoke_endpoint(EndpointName=endpoint_name,
                                           Body=content)
        result = json.loads(response['Body'].read().decode())
        #current_time = datetime.datetime.now(pytz.timezone(time_zone))
        current_time = datetime.now(pytz.timezone(time_zone))
        image_id = str(uuid.uuid4())

        # update dynamodb table
        # NOTE: dynamodb requires float types to be converted to Decimal types
        item = {'IMAGE ID': image_id,
                'DATE ADDED': str(current_time.date()),
                'TIME ADDED': current_time.strftime('%I:%M:%S %p {}'.format(time_zone))}
        # determine the predicted class based on the highest probability returned
        predicted_class = {'class': '', 'probability': 0.0}
        for result_index, probability in enumerate(result):
            item['PROBABILITY OF ' + classes[result_index]] = Decimal(str(round(probability, 8)))
            if probability > predicted_class['probability']:
                predicted_class['class'] = classes[result_index]
                predicted_class['probability'] = probability
            # if two or more classes have the same high probability, then create a concatenated string of the classes
            elif probability == predicted_class['probability']:
                predicted_class['class'] = predicted_class['class'] + ', ' + classes[result_index]
        item['CLASS'] = predicted_class['class']
        db_table.put_item(Item=item)

        # copy image to destination bucket to corresponding folder based on the predicted class
        copy_source = {'Bucket': bucket, 'Key': key}
        destination_bucket = s3_resource.Bucket(destination_bucket_name)
        destination_bucket.copy(copy_source, predicted_class['class'] + '/' + image_id)
        
        # delete uploaded file from source bucket
        _ = s3_client.delete_object(Bucket=bucket, Key=key)
        print('Image Added!\nIMAGE ID: {}\nPREDICTED CLASS: {}'.format(image_id, predicted_class['class']))

