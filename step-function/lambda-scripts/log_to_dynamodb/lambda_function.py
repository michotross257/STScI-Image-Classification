import os
import boto3
from decimal import Decimal
from datetime import datetime, timezone

img_classifications_table_id = os.environ.get('IMAGE_CLASSIFICATIONS_TABLE')
upload_history_table_id = os.environ.get('UPLOAD_HISTORY_TABLE')
db_resource = boto3.resource('dynamodb')
img_classifications_table = db_resource.Table(img_classifications_table_id)
upload_history_table = db_resource.Table(upload_history_table_id)


def lambda_handler(event, context):
    item = {
        "IMAGE ID": event['image_id'],
        "PREDICTED CLASS": event['classification']['predicted_class']
        
    }
    classes = event['classification']['probabilities']
    for cls in classes:
        item['PROBABILITY OF ' + cls] = Decimal(classes[cls])
    current_time = datetime.now(timezone.utc)
    item['DATE ADDED TO TABLE'] = str(current_time.date())
    item['TIME ADDED TO TABLE'] = current_time.strftime('%H:%M:%S %p %Z')
    for key in event['metadata']:
        if type(event['metadata'][key]) is float:
            item[key.upper()] = Decimal(str(round(event['metadata'][key], 8)))
        else:
            item[key.upper()] = event['metadata'][key]
    
    # update DynamoDB tables
    img_classifications_table.put_item(Item=item)
    upload_history_table.put_item(Item={ 'IMAGE ID': event['image_id'] })
