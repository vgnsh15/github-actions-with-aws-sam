import json
# import requests
import boto3
import pandas as pd
from io import BytesIO
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    # try:
    #     ip = requests.get("http://checkip.amazonaws.com/")
    # except requests.RequestException as e:
    #     # Send some context about this error to Lambda Logs
    #     print(e)

    #     raise e
    
    try:
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_name = event["Records"][0]["s3"]["object"]["key"]
        print("S:",bucket_name)
        print("file:",s3_file_name)
        resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
        df_s3_data = pd.read_csv(resp['Body'], sep='\t')

      
        print(df_s3_data.head())

    except Exception as err:
        print(err)
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "output data {data}".format(data=df_s3_data.head()),
            # "location": ip.text.replace("\n", "")
        }),
    }
