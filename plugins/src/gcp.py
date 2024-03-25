import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import json
from pandas_gbq import to_gbq
import pandas as pd
from lib.config_helper import IACConfigHelper
import argparse
from io import BytesIO


def bg_connection(json_object:json):
    client = bigquery.Client.from_service_account_info(json_object)
    return client

def bq_create_table(
    client,
    project_id_str: str,
    dataset_str: str,
    tableName_str: str,
    table_schema: list,
):
    """
    Function to create a table in BigQuery if it does not exist.

    Args:
        client (google.cloud.bigquery.Client): The BigQuery client.
        dataset_str (str): The name of the dataset containing the table.
        table_name (str): The name of the table to create.
        table_schema (list): The schema of the table.

    Returns:
        google.cloud.bigquery.Table: The created or existing table.
    """
    dataset_ref = client.dataset(dataset_str, project=project_id_str)
    # Prepares a reference to the table
    table_ref = dataset_ref.table(tableName_str)
    try:
        table = client.get_table(table_ref)
        print("table {} already exists.".format(table))
    except NotFound:
        table = bigquery.Table(table_ref, schema=table_schema)
        table = client.create_table(table)
        print("table {} created.".format(table.table_id))
    return table 


def import_dataframe_to_bigquery(
    project_id_str: str, dataset_str: str, tableName_str: str, dataframe: pd.DataFrame
):
    """
    Upload data from a Pandas DataFrame to a BigQuery table.

    Args:
        dataset_str (str): The name of the dataset.
        table_name (str): The name of the table.
        dataframe (pd.DataFrame): The Pandas DataFrame to upload.
    """
    # Prepare a reference to the dataset
    destination_table = f"{project_id_str}.{dataset_str}.{tableName_str}"
    to_gbq(dataframe, destination_table, if_exists="replace")
    
from google.cloud import bigquery
import json

def import_json_to_bigquery(client, dataset_name: str, table_name: str, json_objects: json):
    """
    Load data from a JSON file into a BigQuery table.

    Args:
        project_id (str): Google Cloud project ID.
        dataset_name (str): The name of the BigQuery dataset.
        table_name (str): The name of the BigQuery table.
        json_file_path (str): Path to the JSON file containing the data.
    """
    # Construct a BigQuery client object.
    # client = bigquery.Client(project=project_id)
    # Load the JSON  into BigQuery
    table_ref = client.dataset(dataset_name).table(table_name)
    table = client.get_table(table_ref)
    errors = client.insert_rows_json(table, json_objects)
    print(f"Job finished. Loaded data into {table}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract data from Spotify.")
    parser.add_argument(
        "--config", type=str, default="default_value", help="environment"
    )
    args = parser.parse_args()
    conn_config = IACConfigHelper.get_conn_info(args.config)
    credentials_json = json.loads(conn_config['g_service_account']['credentials'])
    client = bg_connection(credentials_json)
    # processed_top_artists_json
    # processed_top_tracks_json
    # processed_current_follow_artists_json
    top_artists_table = 'top_artists'
    top_tracks_table = 'top_tracks'
    current_follow_artists_table = 'current_follow_artists'
    spotify_top_artists_schema = [
        bigquery.SchemaField("uri", "STRING", mode="NULLABLE", description="Spotify URI for the artist"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE", description="Name of the artist"),
        bigquery.SchemaField("type", "STRING", mode="NULLABLE", description="Type of the Spotify entity, e.g., artist"),
        bigquery.SchemaField("followers", "INTEGER", mode="NULLABLE", description="Number of followers on Spotify"),
        bigquery.SchemaField("popularity", "INTEGER", mode="NULLABLE", description="Popularity of the artist on Spotify"),
        bigquery.SchemaField("genres", "STRING", mode="NULLABLE", description="List of genres the artist is associated with, as a comma-separated string"),
        bigquery.SchemaField("spotify_url", "STRING", mode="NULLABLE", description="URL to the artist's Spotify page"),
        bigquery.SchemaField("image_url", "STRING", mode="NULLABLE", description="URL to an image of the artist")
    ]
    spotify_top_tracks_schema = [
        bigquery.SchemaField("song_name", "STRING", mode="NULLABLE", description="Name of the song"),
        bigquery.SchemaField("artist", "STRING", mode="NULLABLE", description="Artist name"),
        bigquery.SchemaField("album", "STRING", mode="NULLABLE", description="Album name"),
        bigquery.SchemaField("release_date", "DATE", mode="NULLABLE", description="Release date of the song"),
        bigquery.SchemaField("total_tracks_in_album", "INTEGER", mode="NULLABLE", description="Total tracks in the album"),
        bigquery.SchemaField("song_duration", "INTEGER", mode="NULLABLE", description="Duration of the song in milliseconds"),
        bigquery.SchemaField("popularity", "INTEGER", mode="NULLABLE", description="Popularity of the song"),
        bigquery.SchemaField("spotify_album_url", "STRING", mode="NULLABLE", description="Spotify URL for the album"),
        bigquery.SchemaField("preview_url", "STRING", mode="NULLABLE", description="Preview URL of the song"),
        bigquery.SchemaField("image_url", "STRING", mode="NULLABLE", description="Image URL for the song/album"),
    ]
    spotify_current_follow_artists_schema = [
        bigquery.SchemaField("uri", "STRING", description="Unique Resource Identifier for the artist on Spotify", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", description="Name of the artist", mode="NULLABLE"),
        bigquery.SchemaField("type", "STRING", description="Type of the Spotify object, should be 'artist'", mode="NULLABLE"),
        bigquery.SchemaField("followers", "INTEGER", description="Number of followers the artist has on Spotify", mode="NULLABLE"),
        bigquery.SchemaField("popularity", "INTEGER", description="Popularity of the artist on Spotify, scale of 0 to 100", mode="NULLABLE"),
        bigquery.SchemaField("genres", "STRING", description="Genres the artist is associated with, as a comma-separated string", mode="NULLABLE"),
        bigquery.SchemaField("spotify_url", "STRING", description="URL to the artist's Spotify page", mode="NULLABLE"),
        bigquery.SchemaField("image_url", "STRING", description="URL to an image of the artist", mode="NULLABLE")
    ]
    
    bq_create_table(client,conn_config['bigquery']['project_id'],conn_config['bigquery']['dataset'],top_artists_table,spotify_top_artists_schema)
    bq_create_table(client,conn_config['bigquery']['project_id'],conn_config['bigquery']['dataset'],top_tracks_table,spotify_top_tracks_schema)
    bq_create_table(client,conn_config['bigquery']['project_id'],conn_config['bigquery']['dataset'],current_follow_artists_table,spotify_current_follow_artists_schema)

    # import_json_to_bigquery(project_id, dataset_name, table_name, json_content)
