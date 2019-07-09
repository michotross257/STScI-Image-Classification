import os
from datetime import datetime, timezone
from decimal import Decimal
import uuid
import boto3
from google.cloud import automl_v1beta1 as automl


# --------------------------- AUTOML ---------------------------
project_id = os.environ.get('AUTOML_PROJECT_ID')
model_id = os.environ.get('AUTOML_MODEL_ID')
compute_region = os.environ.get('AUTOML_COMPUTE_REGION')
# the level of confidence the model must have to return the results for a class label
score_threshold = '0.0' # value from 0.0 to 1.0; default is 0.5
# --------------------------- AWS ---------------------------
table_id = os.environ.get('IMAGE_CLASSIFICATIONS_TABLE')
destination_bucket_name = os.environ.get('DESTINATION_BUCKET')
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
db_resource = boto3.resource('dynamodb')
db_table = db_resource.Table(table_id)


def get_prediction(content, project_id, model_id):
    '''Pass an image to automl model for classification.
       Function returns an automl_v1beta1.types.PredictResponse object.'''
    automl_client = automl.AutoMlClient()
    model_full_id = automl_client.model_path(project_id, compute_region, model_id)
    prediction_client = automl.PredictionServiceClient()
    payload = {'image': {'image_bytes': content}}
    params = {'score_threshold': score_threshold}
    response = prediction_client.predict(model_full_id, payload, params)
    return response


def lambda_handler(event, context):
    '''For each new image added to source bucket:
            1) pass to Google automl for classification
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
        output = get_prediction(content, project_id, model_id)
        image_id = str(uuid.uuid4())
        current_time = datetime.now(timezone.utc)
        
        # update dynamodb table
        # NOTE: dynamodb requires float types to be converted to Decimal types
        item = {'IMAGE ID': image_id,
                'DATE ADDED': str(current_time.date()),
                'TIME ADDED': current_time.strftime('%I:%M:%S %p {}'.format(current_time.tzname()))}
        # determine the predicted class based on the highest probability returned
        predicted_class = {'class': '', 'probability': 0.0}
        for payload in output.payload:
            item['PROBABILITY OF ' + payload.display_name] = Decimal(str(round(payload.classification.score, 8)))
            if payload.classification.score > predicted_class['probability']:
                predicted_class['class'] = payload.display_name
                predicted_class['probability'] = payload.classification.score
            # if two or more classes have the same high probability, then create a concatenated string of the classes
            elif payload.classification.score == predicted_class['probability']:
                predicted_class['class'] = predicted_class['class'] + ', ' + payload.display_name
        item['CLASS'] = predicted_class['class']
        db_table.put_item(Item=item)

        # copy image to destination bucket to corresponding folder based on the predicted class
        copy_source = {'Bucket': bucket, 'Key': key}
        destination_bucket = s3_resource.Bucket(destination_bucket_name)
        destination_bucket.copy(copy_source, predicted_class['class'] + '/' + image_id)
        
        # delete uploaded file from source bucket
        _ = s3_client.delete_object(Bucket=bucket, Key=key)
        print('Image Added!\nIMAGE ID: {}\nPREDICTED CLASS: {}'.format(image_id, predicted_class['class']))

