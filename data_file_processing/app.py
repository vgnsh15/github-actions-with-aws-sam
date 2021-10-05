import json
# import requests
import boto3
import pandas as pd
import pandasql as ps
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
        df= pd.read_csv(resp['Body'], sep='\t')
        
        #df = pd.read_csv("data_4_63_84_41.tsv", usecols=['ip','pagename', 'page_url', 'product_list', 'referrer','event_list','hit_time_gmt'], sep='\t')
        df['search_domain'] = df['referrer'].str.extract(r'(https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+)')
        df['search_key'] = df['referrer'].str.extract(r'\W*\\?=([^&#]*)')
        df['product_list'] = df['product_list'].fillna("None")
        df['event_list'] = df['event_list']
        df['product_list_new'] = df['product_list'].fillna("None").str.split(';').str[3]
        formatted_df = df[['ip','search_key','event_list', 'product_list_new', 'search_domain','hit_time_gmt']]
        purchased_df_query = """SELECT ip FROM formatted_df where event_list = 1 order by ip,hit_time_gmt  """
        df_hit_query = """SELECT * FROM formatted_df f_df inner join purchased_df_query p_df on f_df.ip=p_df.ip order by ip,hit_time_gmt  """
        purchased_df = ps.sqldf(purchased_df_query, locals())
        join_df = pd.merge(formatted_df, purchased_df,how='inner', on=['ip'])[['ip','search_key','search_domain','product_list_new']]
        join_df.rename({'product_list_new': 'revenue'}, axis=1, inplace=True)
        revenue_df=join_df.groupby(['ip','revenue'])['search_key','search_domain'].agg(list)
        print(revenue_df)

      
        print(revenue_df.head())
        
        with io.StringIO() as csv_buffer:
            revenue_df.to_csv(csv_buffer, index=False)

            response = s3_client.put_object(Bucket='adbassessmwnt', Key="output_files/revenue.csv", Body=csv_buffer.getvalue())

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
        
    except Exception as err:
        print(err)
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "output data {data}".format(data=revenue_df.head()),
            # "location": ip.text.replace("\n", "")
        }),
    }
