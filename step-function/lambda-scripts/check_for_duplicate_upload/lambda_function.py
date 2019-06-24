import os
import boto3


table_id = os.environ.get('UPLOAD_HISTORY_TABLE')
db_resource = boto3.resource('dynamodb')
upload_history_table = db_resource.Table(table_id)


def lambda_handler(event, context, call=None, callback=None):
    response = event['detail']['requestParameters']
    bucket = response['bucketName']
    key = response['key']
    
    print("Bucket: {}\nKey: {}".format(bucket, key))
    
    # image id is equal to the key minus the file extension, so get rid of extension
    index = -1
    while key[index] != '.':
        index -= 1
    image_id = key[:index]

    # to avoid duplicate uploads, check to see if image has been previously uploaded
    query = upload_history_table.query(
       ProjectionExpression="#img_id",
       ExpressionAttributeNames={ "#img_id": "IMAGE ID" },
       KeyConditionExpression="#img_id = :id",
       ExpressionAttributeValues={":id": image_id}
    )

    print('\nQuery Result:', query['Items'])
    is_duplicate = True if len(query['Items']) else False

    return {
        'image_id': image_id,
        'is_duplicate': is_duplicate,
        's3': {
            'key': key,
            'bucket': bucket
        }
    }
