import os
import sys
import yaml
import logging
import importlib

from datetime import datetime, timedelta

from searchtweets import ResultStream, gen_request_parameters, load_credentials
from tenacity import after_log, retry, stop_after_attempt, wait_exponential

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from mongo_utils import mongo_utils

logger = logging.getLogger(__name__)

# Load config and credentials

config_all = yaml.safe_load(open("../config_files/config.yaml"))

mongo_cred_path = os.path.join(
    os.path.dirname(__file__),
    "../config_files",
    config_all["mongodb_params"]["mongodb_cred_filename"],
)
mongodb_credentials = yaml.safe_load(open(mongo_cred_path))["mongodb_credentials"]

twitter_cred_path = os.path.join(
    os.path.dirname(__file__),
    "../config_files",
    config_all["research_params"]["twitter_cred_filename"],
)
twitter_credentials = yaml.safe_load(open(twitter_cred_path))["search_tweets_api"]


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=60 * 10),
    stop=stop_after_attempt(10),
    after=after_log(logger, logging.INFO),
)
def _search_twitter(rule, max_tweets, output_format, twitter_credentials):
    rs = ResultStream(
        request_parameters=rule,
        max_tweets=max_tweets,
        output_format="a",
        **twitter_credentials
    )
    print(rs)
    results = rs.stream()
    for i in results:
        yield i


def search_twitter(twitter_credentials, rule_params):
    def prepare_rule(params):
        for k in params:
            if isinstance(params[k], list):
                if len(params[k]) > 0:
                    if k != "query" and k != "additional_query":
                        params[k] = ",".join(params[k])
                else:
                    params[k] = None
        params['query']= "({})".format(" OR ".join(params['query'])) +  ' ' + ' '.join(params['additional_query'])
        return params

    def prepare_time(params):

        if params["date"] is not None and params["since_id"] is None:

            date = datetime.strptime(params["date"], "%d-%m-%Y")

            if params['days_before'] == 0 and params['days_after'] == 0:
                start_time = date - timedelta(minutes=1)
                end_time = date + timedelta(hours=23, minutes=59)
            else:
                start_time = date - timedelta(days=params["days_before"])
                end_time = date + timedelta(days=params["days_after"], hours=23, minutes=59)

            params["start_time"] = start_time.strftime("%Y-%m-%d %H:%M")
            params["end_time"] = end_time.strftime("%Y-%m-%d %H:%M")
        else:
            pass
        return params

    def delete_uneeded_keys(params):
        del (
            params["date"],
            params["days_before"],
            params["days_after"],
            params["twitter_cred_filename"],
            params["max_tweets"],
            params["output_format"],
            params['additional_query']

        )
        return params

    max_tweets = rule_params["max_tweets"]
    output_format = rule_params["output_format"]
    rule_params = prepare_rule(rule_params)
    rule_params = prepare_time(rule_params)
    rule_params = delete_uneeded_keys(rule_params)

    rule = gen_request_parameters(**rule_params)
    return _search_twitter(
        rule,
        max_tweets,
        output_format=output_format,
        twitter_credentials=twitter_credentials,
    )


def insert_tweets_mongo(tweet, collection):

    collection.update_one({"id": tweet["id"]}, {"$set": tweet}, upsert=True)


# # TODO write an aggregate rather than loop but not really important
# def get_ref_tweet_list(col_tweets):
#     """
#     Get the original tweet id when it is a RT to replace the truncatedtry/except
#     """
#     set_unique_ids = set()
#     for tweet in col_tweets.find(
#         {"referenced_tweets": {"$elemMatch": {"type": "retweeted"}}}
#     ):
#         set_unique_ids.add(tweet["referenced_tweets"][0]["id"])
#     return set_unique_ids


def main():
    mydb = mongo_utils.get_mongo_db()
    col_tweet_name = config_all["mongodb_params"]["col_name"]
    col_tweets = mydb[col_tweet_name]
    col_tweets.create_index([("id", 1)], unique=True)
    rule_params = config_all['research_params']
    dates_to_parse = rule_params['date']
    for date in dates_to_parse:
        rule_params['date'] = date
        tweets = search_twitter(
            twitter_credentials, rule_params=rule_params.copy()
        )
        for tweet in tweets:
            insert_tweets_mongo(tweet, col_tweets)

    # set_unique_ids = get_ref_tweet_list(col_tweets)
    # print(len(set_unique_ids))


if __name__ == "__main__":
    main()
