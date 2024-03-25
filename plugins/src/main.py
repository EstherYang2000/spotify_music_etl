import os
# from config_helper import IACConfigHelper
import argparse
from spotify import spotify_auth,extract_top_artists,extract_top_tracks,extract_current_follow_artists
from gcp import bg_connection,import_json_to_bigquery
import json
import logging
import os
import yaml

class IACConfigHelper:
    @staticmethod
    def get_conn_info(conn_file):
        with open(conn_file, "r") as file:
            conn_config = yaml.safe_load(file)

        return conn_config

    @staticmethod
    def set_google_cloud_key(key_file):
        if not os.path.isfile(key_file):
            raise Exception(f"GOOGLE_APPLICATION_CREDENTIALS file not found: {key_file}")
        if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file
            print(f"Set the GOOGLE_APPLICATION_CREDENTIALS to {key_file}")
        print(f"GOOGLE_APPLICATION_CREDENTIALS PATH: {key_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract data from Spotify.")
    parser.add_argument(
        "--config", type=str, default="default_value", help="environment"
    )
    args = parser.parse_args()
    conn_config = IACConfigHelper.get_conn_info(args.config)
    sp = spotify_auth(conn_config['spotify']['client_id'],conn_config['spotify']['client_secret'],conn_config['spotify']['redirect_url'],conn_config['spotify']['scope'])
    print(sp)
    credentials_json = json.loads(conn_config['g_service_account']['credentials'])
    client = bg_connection(credentials_json)
    print(client)
    top_artists_data = extract_top_artists(sp)
    top_tracks_data = extract_top_tracks(sp)
    current_follow_artists_data = extract_current_follow_artists(sp)
    import_json_to_bigquery(client, "spotify", "top_artists", top_artists_data)
    import_json_to_bigquery(client, "spotify", "top_tracks", top_tracks_data)
    import_json_to_bigquery(client, "spotify", "current_follow_artists", current_follow_artists_data)