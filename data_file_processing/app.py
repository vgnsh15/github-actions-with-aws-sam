import json
# import requests
import boto3
import pandas as pd
import pandasql as ps
import numpy as np
from pandasql import sqldf
import io
from io import BytesIO
s3_client = boto3.client('s3')

class Events:
    """
    A class used to represent the Transaction Events

    ...
    Methods
    -------
    revenue_cal(X):
    
        Input_Arguments:  Dataframe Column values <-- revenue
        Output:  The sum of all revenue for a particular transaction
        
        Description: Method which calculates the sum of all the revenue of a particular transaction
    
    purchase_trans_filter(X)
    
        Input_Arguments:  Dataframe Column values <-- event_list
        Output:  Scans the event list for a purchase transaction (1)
        
        Description: Method which scans the event list dataframe column and filter the purchase transaction.
    """
    
    def revenue_cal(self,x):
        """
         
        This Method iterates through the product list attribute of the transaction 
        and sums up all the revenue of the transaction if a purchase happens

        Parameters
        ----------
        X: dataframe column values, required

        Returns
        ------
        Total Sum: Integer
        """
        
        sum=0
        for val in str(x).split(','):
            revenue = str(val).split(';')
            if len(revenue)>3 and (revenue[3]!=None and revenue[3]!="" and revenue[3]!=" "):
                sum+=float(revenue[3].strip())
        return sum
    

    def purchase_trans_filter(self,x):
        
        """
         
        This Method iterates through the event list attribute of the transaction 
        and filters the values for a purchase transactions.

        Parameters
        ----------
        X: dataframe column values, required

        Returns
        ------
        Purchase Transaction: Integer
        
        """
        
        
        event=[]
        for event_trans in str(x).split(','):
            if str(event_trans) is None or str(event_trans) == "" or str(event_trans) == " " or str(event_trans) == "NaN" or str(event_trans) == "nan":
                pass
            elif int(float(event_trans)) == 1:
                event_trans = int(float(event_trans))
                print(event_trans)
                return event_trans
    
    
    def transaction_events(self,event,context):
        """
         
        This Method is the method which handles the data processing capability for the incoming S3 files.
        1. Reads the S3 tab delimited files and creates a dataframe
        2. Uses Pandas library to clean, wrangle, extract the necessary attributes.
        3. Uses Pandas Sqldf to rank the transaction based on ip and hit time and picks the first and last transaction
        to find the search key word, search domain and total revenue generated from the search domain.

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
            Pandas dataframe buffered to write a csv file to a S3 bucket
        
        """
        
        try:
            """
            The bucket name and the S3 file name are fetched from the in-built the event trigger.
            """
            bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
            s3_file_name = event["Records"][0]["s3"]["object"]["key"]
            print("S:",bucket_name)
            print("file:",s3_file_name)
            resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
            df= pd.read_csv(resp['Body'], sep='\t')
            """
            Calling the revenue_cal method using map function 
            """
            df['revenue'] = df['product_list'].map(self.revenue_cal)
            """
            Calling the purchase_trans_filter method using map function 
            """
            df['even_list_values'] = df['event_list'].map(self.purchase_trans_filter)
            """
            Extracting the search domain from the referrer URL
            """
            df['search_domain'] = df['referrer'].str.extract(r'(https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+)')
            """
            Extracting the search key from the referrer URL
            """
            df['search_key'] = df['referrer'].str.extract(r'\W*\\?=([^&#]*)')
            formatted_df = df[['ip','search_key','even_list_values', 'revenue', 'search_domain','hit_time_gmt']]
            purchased_df_query = """SELECT ip FROM formatted_df where even_list_values = 1 order by ip,hit_time_gmt  """
            #df_hit_query = """SELECT * FROM formatted_df f_df inner join purchased_df_query p_df on f_df.ip=p_df.ip where search_key !='nan' order by ip,hit_time_gmt  """
            purchased_df = ps.sqldf(purchased_df_query, locals())
            join_df = pd.merge(formatted_df, purchased_df,how='inner', on=['ip'])[['ip','search_key','search_domain','revenue','hit_time_gmt']]
            join_df.dropna()
            """
            Pandas native methods for Window functions to claculate rank based on partition by ip and order by hit time
            """
            join_df['RN'] = join_df.sort_values(['hit_time_gmt'], ascending=[True]).groupby(['ip']).cumcount() + 1
            """
            Pandas native methods for Window functions to claculate sum based on partition by ip
            """
            join_df['total_revenue'] = join_df.groupby('ip').revenue.transform(np.sum)
            
            final_sql="""select search_key,search_domain,total_revenue from join_df where RN=1;"""
            
            revenue_df = ps.sqldf(final_sql,locals())
           
            """
            Converting the final dataframe to csv buffer and drop the data to the S3 bucket
            """
            with io.StringIO() as csv_buffer:
                revenue_df.to_csv(csv_buffer, sep ='\t', index=False)

                response = s3_client.put_object(Bucket='adbassessment', Key="output_files/revenue.csv", Body=csv_buffer.getvalue())

                status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                """
                On Success upload to S3, the success messsage is passed as na output from the Lambda
                """
                if status == 200:
                    print(f"Successful S3 put_object response. Status - {status}")
                    return {
                        "statusCode": status,
                        "body": json.dumps({
                        "message": "Successful S3 put_object response",
                        # "location": ip.text.replace("\n", "")
                           }),
                    }
 
                else:
                    """
                    Error mesage along with Status code is passed for debugging.
                    """
                    print(f"Unsuccessful S3 put_object response. Status - {status}")
                    return {
                        "statusCode": status,
                        "body": json.dumps({
                        "message": "Something went wrong in the processing",
                           }),
                    }
        
        except Exception as err:
            print(err)

def lambda_handler(event, context):
    """
    This is the main handler method which gets invoked by the lambda on trigger from S3 file drop.
    
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
    
    Events().transaction_events(event,context)
