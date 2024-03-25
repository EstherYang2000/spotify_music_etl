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