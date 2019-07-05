import os
import boto3


table_id = os.environ.get('UPLOAD_HISTORY_TABLE')
db_resource = boto3.resource('dynamodb')
upload_history_table = db_resource.Table(table_id)


def lambda_handler(event, context, call=None, callback=None):
    # to avoid duplicate uploads, check to see if image has been previously uploaded
    query = upload_history_table.query(
       ProjectionExpression="#img_id",
       ExpressionAttributeNames={ "#img_id": "IMAGE ID" },
       KeyConditionExpression="#img_id = :id",
       ExpressionAttributeValues={":id": event['image_id']}
    )

    is_duplicate = True if len(query['Items']) else False
    event['is_duplicate'] = is_duplicate

    return event
