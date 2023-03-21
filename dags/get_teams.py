from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import json
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 17),
    'retries': 1,
}

def upload_to_s3():


    for day in range(1,32):

        date = f'2022-08-{day:02}'
        url = f"https://baseballsavant.mlb.com/statcast_search/csv?hfPTM=&hfPT=&hfAB=&hfGT=R%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2022%7C&hfSit=&player_type=batter&hfOuts=&hfOpponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={date}&game_date_lt={date}&hfMo=&hfTeam=TOR%7C&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=name&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc"

        response = requests.get(url)
        if response.content:

            # Encode the content of the response as a string using 'utf-8'
            response_str = response.content.decode('utf-8')
                
            # Convert the JSON data to a string
            # json_str = json.dumps(json_data)
            
            # Instantiate the S3Hook with your AWS credentials
            s3_hook = S3Hook(aws_conn_id='aws_default')
            
            # Specify the S3 bucket and key where you want to store the JSON file
            bucket_name = 'toronto-blue-jays-baseball'
            key = f'teams-info/bluejays_{date}.csv'
            
            # Use the load_string method of the S3Hook to store the JSON string to S3
            s3_hook.load_string(response_str, key, bucket_name)

with DAG('json_to_s3_dag', 
         default_args=default_args, 
         schedule_interval=None) as dag:

    # Define the PythonOperator to execute the upload_to_s3 function
    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )

upload_task
