# Credential loading
import importlib.util
import os

import pymongo
import yaml

config_all = yaml.safe_load(open("../config_files/config.yaml"))
mongo_cred_path = os.path.join(
    os.path.dirname(__file__),
    "../config_files",
    config_all["mongodb_params"]["mongodb_cred_filename"],
)
mongodb_credentials = yaml.safe_load(open(mongo_cred_path))["mongodb_credentials"]

# Global client
global _mongoclient
_mongoclient = None


def get_mongo_db(db=None):
    """
    Gets the specified Mongo database.
    If not specified, gets the default database from the credentials file.
    """
    if db:
        database = db
    else:
        database = mongodb_credentials["DB_MONGO_DATABASE"]
    client = get_client()
    return client[database]


def get_client():
    """
    Gets the MongoClient Object. If Mongo is not connected, connects.
    """
    global _mongoclient
    if _mongoclient == None:
        host = mongodb_credentials["DB_HOST"]

        port = int(mongodb_credentials["DB_MONGO_PORT"])
        try:
            user = mongodb_credentials["DB_MONGO_USER"]
        except KeyError:
            user = None
        try:
            passw = mongodb_credentials["DB_MONGO_PASS"]
        except KeyError:
            passw = None
        _mongoclient = pymongo.MongoClient(host, port, username=user, password=passw)
    return _mongoclient
