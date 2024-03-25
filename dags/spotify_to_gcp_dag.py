# This is a conceptual snippet; actual implementation will vary
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag, task, task_group
from datetime import datetime,timedelta
from airflow.models import Variable
from lib.config_helper import IACConfigHelper
from src.spotify import spotify_auth,extract_top_artists,extract_top_tracks,extract_current_follow_artists
from src.gcp import bg_connection,import_json_to_bigquery
import json

@dag(schedule_interval=None, start_date=datetime(2024, 3, 16))
def spotify_etl_dag():
    @task_group
    def set_config():
        @task()
        def set_variable():
            Variable.set(key="config_path", value="config/config.yaml")
        @task()
        def get_variable():
            config_path_var = Variable.get("config_path")
            print(f"config_path_var: {config_path_var}")
        @task()
        def read_config():
            config_path_var = Variable.get("config_path")
            conn_config = IACConfigHelper.get_conn_info(config_path_var)
            return conn_config
        set_variable()
        get_variable()
        conn_config = read_config()
        print(conn_config)
        return conn_config
        
    # @task()
    # def spotify_authenticate(conn_config):
    #     sp = spotify_auth(
    #         client_id=conn_config['spotify']['client_id'],
    #         client_secret=conn_config['spotify']['client_secret'],
    #         redirect_uri=conn_config['spotify']['redirect_url'],
    #         scope=conn_config['spotify']['scope']
    #     )
    #     return {"sp":sp}
    # @task()
    # def bq_auth(conn_config):
    #     credentials_json = json.loads(conn_config['g_service_account']['credentials'])
    #     client = bg_connection(credentials_json)
    #     return client 
    # @task_group
    def extract_data(conn_config):
        # sp = spotify_auth(
        #         client_id=conn_config['spotify']['client_id'],
        #         client_secret=conn_config['spotify']['client_secret'],
        #         redirect_uri=conn_config['spotify']['redirect_url'],
        #         scope=conn_config['spotify']['scope']
        #     )
        @task(multiple_outputs=True) 
        def extract_data_etl(conn_config):
            sp = spotify_auth(
                client_id=conn_config['spotify']['client_id'],
                client_secret=conn_config['spotify']['client_secret'],
                redirect_uri=conn_config['spotify']['redirect_url'],
                scope=conn_config['spotify']['scope']
                
            )
            processed_top_artists_json = extract_top_artists(sp)
            processed_top_tracks_json = extract_top_tracks(sp)
            processed_current_follow_artists_json = extract_current_follow_artists(sp)
            # if isinstance(processed_top_artists_json, (dict, list)):
            #     json_str = json.dumps(processed_top_artists_json)
            #     print(type(json_str))
            return {"top_artists":processed_top_artists_json,"top_tracks":processed_top_tracks_json,"current_follow_artists":processed_current_follow_artists_json}
            # else:
            #     raise ValueError("Data returned from extract_top_artists() is not serializable")

        # @task(multiple_outputs=True)
        # def extract_top_tracks_etl(conn_config):
        #     sp = spotify_auth(
        #         client_id=conn_config['spotify']['client_id'],
        #         client_secret=conn_config['spotify']['client_secret'],
        #         redirect_uri=conn_config['spotify']['redirect_url'],
        #         scope=conn_config['spotify']['scope']
        #     )
        #     processed_top_tracks_json = extract_top_tracks(sp)
        #     return processed_top_tracks_json
        # @task(multiple_outputs=True)
        # def extract_current_follow_artists_etl(conn_config):
        #     sp = spotify_auth(
        #         client_id=conn_config['spotify']['client_id'],
        #         client_secret=conn_config['spotify']['client_secret'],
        #         redirect_uri=conn_config['spotify']['redirect_url'],
        #         scope=conn_config['spotify']['scope']
        #     )
        #     processed_current_follow_artists_json = extract_current_follow_artists(sp)
        #     return processed_current_follow_artists_json
        processed_json = extract_data_etl(conn_config)
        # processed_top_tracks_json = extract_top_tracks_etl(conn_config)
        # processed_current_follow_artists_json = extract_current_follow_artists_etl(conn_config)
        return processed_json
    @task
    def load_top_artists_to_gcp(conn_config,processed_top_artists_json):
        credentials_json = json.loads(conn_config['g_service_account']['credentials'])
        client = bg_connection(credentials_json)
        import_json_to_bigquery(client, conn_config['bigquery']['dataset'], conn_config['bigquery']['table1'], processed_top_artists_json)
        pass
    @task
    def load_top_tracks_to_gcp(conn_config,processed_top_tracks_json):
        credentials_json = json.loads(conn_config['g_service_account']['credentials'])
        client = bg_connection(credentials_json)
        import_json_to_bigquery(client, conn_config['bigquery']['dataset'], conn_config['bigquery']['table2'], processed_top_tracks_json)
        pass
    @task
    def load_current_follow_artists_to_gcp(conn_config,processed_current_follow_artists_json):
        credentials_json = json.loads(conn_config['g_service_account']['credentials'])
        client = bg_connection(credentials_json)
        import_json_to_bigquery(client, conn_config['bigquery']['dataset'], conn_config['bigquery']['table3'], processed_current_follow_artists_json)
        pass
    conn_config = set_config()
    processed_json = extract_data(conn_config)
    # sp = spotify_authenticate(conn_config)
    # client = bq_auth(conn_config)
    # processed_top_artists_json = extract_top_artists_etl(conn_config)["top_artists_json"]
    load_top_artists_to_gcp(conn_config,processed_json['top_artists'])
    # processed_top_tracks_json = extract_top_tracks_etl(conn_config)["top_tracks_json "]
    load_top_tracks_to_gcp(conn_config,processed_json['top_tracks'])
    # processed_current_follow_artists_json = extract_current_follow_artists_etl(conn_config)["current_follow_artists_json"]
    load_current_follow_artists_to_gcp(conn_config,processed_json['current_follow_artists'])
    # extract_top_artists >> load_top_artists_to_gcp
    # extract_top_tracks >> load_top_tracks_to_gcp
    # extract_current_follow_artists >> load_current_follow_artists_to_gcp
    # load_data_to_gcp(client,conn_config,processed_top_artists_json,processed_top_tracks_json,processed_current_follow_artists_json)
spotify_etl_dag()

# default_args = {
#     'owner': 'airflow',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=10),
# }
# # dag = DAG('spotify_data_pipeline', start_date=datetime(2024, 3, 16))

# with DAG(
#     'spotify_data_pipeline',
#     default_args=default_args,
#     description='A DAG for Spotify data extraction and loading to GCP',
#     schedule_interval='@daily',
#     start_date=datetime(2024, 3, 16),
#     catchup=False,
# ) as dag:

#     set_var_task = PythonOperator(
#         task_id='set_var',
#         python_callable=set_variable
#     )

#     get_var_task = PythonOperator(
#         task_id='get_var',
#         python_callable=get_variable
#     )

#     spotify_authenticate_task = PythonOperator(
#         task_id='spotify_authenticate',
#         python_callable=spotify_authenticate,
#         provide_context=True,
#     )

#     extract_top_artists_task = PythonOperator(
#         task_id='extract_top_artists_task',
#         python_callable=extract_top_artists,
#         op_kwargs={'task_id': 'extract_top_artists', 'extraction_callable': extract_top_artists},
#         provide_context=True,
#     )

#     extract_top_tracks_task = PythonOperator(
#         task_id='extract_top_tracks_task',
#         python_callable=extract_top_tracks,
#         op_kwargs={'task_id': 'extract_top_tracks', 'extraction_callable': extract_top_tracks},
#         provide_context=True,
#     )

#     extract_current_follow_artists_task = PythonOperator(
#         task_id='extract_current_follow_artists_task',
#         python_callable=extract_current_follow_artists,
#         op_kwargs={'task_id': 'extract_current_follow_artists', 'extraction_callable': extract_current_follow_artists},
#         provide_context=True,
#     )
#     bq_auth_task = PythonOperator(
#         task_id='bq_auth_task',
#         python_callable=bq_auth,
#         op_kwargs={'task_id': 'extract_current_follow_artists', 'extraction_callable': bq_auth},
#         provide_context=True,
#     )

#     load_data_to_gcp_task = PythonOperator(
#         task_id='load_data_to_gcp',
#         python_callable=load_data_to_gcp,
#         provide_context=True,
#     )

    # Task dependencies
    # set_var_task >> get_var_task >> spotify_authenticate_task >> [extract_top_artists_task, extract_top_tracks_task, extract_current_follow_artists_task] >> bq_auth_task >> load_data_to_gcp_task