import base64
import json
import boto3

def lambda_handler(event, context):
  for record in event['Records']:
    payload=base64.b64decode(record["kinesis"]["data"])
    result = json.loads(payload)
  
    write_to_db(result)
    print("Object successfully stored in DB.")
       
def write_to_db(data):
  dynamodb = boto3.resource('dynamodb', region_name="ap-south-1")
  table = dynamodb.Table("users")
  table.put_item(Item=data)