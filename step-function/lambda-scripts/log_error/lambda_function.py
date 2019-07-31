import os
import boto3
import json
from datetime import datetime, timezone


table_id = os.environ.get('ERROR_TABLE')
db_resource = boto3.resource('dynamodb')
table = db_resource.Table(table_id)


def lambda_handler(event, context):
    try:
        error = json.loads(event['error-info']['Cause'])
    except:
        error = event['error-info']['Cause']
    
    item = {
        "IMAGE ID": event['image_id'],
        "KEY": event['s3']['key'],
        "BUCKET": event['s3']['bucket'],
        "ERROR TYPE": error['errorType'],
        "ERROR MESSAGE": error['errorMessage']
    }
    
    current_time = datetime.now(timezone.utc)
    item['DATE ADDED TO TABLE'] = str(current_time.date())
    item['TIME ADDED TO TABLE'] = current_time.strftime('%H:%M:%S %p %Z')

    # add to DynamoDB table
    table.put_item(Item=item)
    
    print("Error Type: {}\nError Message: {}".format(error['errorType'],
                                                     error['errorMessage']))
