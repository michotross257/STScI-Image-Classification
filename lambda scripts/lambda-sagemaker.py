import os
import json
import boto3
from decimal import Decimal
from datetime import datetime, timezone


# environment variables
endpoint_name = os.environ.get('ENDPOINT_NAME')
img_classifications_table_id = os.environ.get('IMAGE_CLASSIFICATIONS_TABLE')
upload_history_table_id = os.environ.get('UPLOAD_HISTORY_TABLE')
destination_bucket_name = os.environ.get('DESTINATION_BUCKET')
classes = [cls.strip() for cls in os.environ.get('CLASSES').split(',')]

# acquire AWS service access
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
runtime = boto3.client('runtime.sagemaker')
db_resource = boto3.resource('dynamodb')
img_classifications_table = db_resource.Table(img_classifications_table_id)
upload_history_table = db_resource.Table(upload_history_table_id)


def lambda_handler(event, context):
    '''
        For each new image added to upload bucket:
            Check to see if upload is a duplicate
            a) if it is a duplicate, then...
                1) delete image from upload bucket
                2) log info
            b) if it is not a duplicate, then...
                1) pass to SageMaker endpoint for classification
                2) copy image to appropriate folder in destination bucket
                3) delete original image from source bucket
                4) update DynamoDB tables
                5) log classification info
       
       Returns:
           None
    '''
    for index, record in enumerate(event['Records']):
        message = json.loads(record['Sns']['Message'])
        bucket = message['Records'][0]['s3']['bucket']['name']
        key = message['Records'][0]['s3']['object']['key']
        # image id is equal to the key minus the file extension, so get rid of extension
        index = -1
        while key[index] != '.':
            index -= 1
        image_id = key[:index]
        
        # to avoid duplicate uploads, check to see if image has been previously uploaded
        response = upload_history_table.query(
            ProjectionExpression="#img_id",
            ExpressionAttributeNames={ "#img_id": "IMAGE ID" },
            KeyConditionExpression="#img_id = :id",
            ExpressionAttributeValues={":id": image_id})
        
        # if the image id is found, then delete image from upload bucket and bypass the remaining lambda actions
        if len(response['Items']):
            # delete uploaded file from source bucket
            _ = s3_client.delete_object(Bucket=bucket, Key=key)
            print('Duplicate upload attempted and avoided.\nIMAGE ID: {}'.format(image_id))
        else:
            download_path = '/tmp/{}'.format(key)
            # download image and send off to SageMaker for classification
            s3_client.download_file(bucket, key, download_path)
            with open(download_path, 'rb') as downloaded_file:
                content = downloaded_file.read()
            response = runtime.invoke_endpoint(EndpointName=endpoint_name,
                                               Body=content)
            result = json.loads(response['Body'].read().decode())
            current_time = datetime.now(timezone.utc)
    
            # NOTE: DynamoDB requires float types to be converted to Decimal types
            item = {'IMAGE ID': image_id,
                    'DATE ADDED': str(current_time.date()),
                    'TIME ADDED': current_time.strftime('%H:%M:%S %p %Z')}
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
    
            # copy image to destination bucket to corresponding folder based on the predicted class
            copy_source = {'Bucket': bucket, 'Key': key}
            destination_bucket = s3_resource.Bucket(destination_bucket_name)
            destination_bucket.copy(copy_source, predicted_class['class'] + '/' + image_id)
            
            # delete uploaded file from source bucket
            _ = s3_client.delete_object(Bucket=bucket, Key=key)
            
            # update DynamoDB tables
            img_classifications_table.put_item(Item=item)
            upload_history_table.put_item(Item={ 'IMAGE ID': image_id })
            
            print('Image Added!\nIMAGE ID: {}\nPREDICTED CLASS: {}'.format(image_id, predicted_class['class']))
