import importlib
import os
import sys
from datetime import datetime, timedelta

import config
from twitter_search import TweetSearchUtil

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from mongo_utils import mongo_utils


cred_path = os.path.join(os.path.dirname(__file__), "../credentials.py")
spec = importlib.util.spec_from_file_location("credentials", cred_path)
credentials = importlib.util.module_from_spec(spec)
spec.loader.exec_module(credentials)
mongodb_credentials = credentials.mongodb_credentials()

# for testing purpose limited these are limited now
MAX_TWEETS_RETRIEVED = 10000
# MAX_CLAIMS_PER_DAY = 5


def search_twitter(query, date=None, days_before=0, days_after=0):
    tsu = TweetSearchUtil("twittercredentials.yaml")
    tweet_fields = "author_id,conversation_id,in_reply_to_user_id,referenced_tweets,created_at,geo,id,lang,public_metrics,text"
    if date is None:
        tweets = tsu.search_tweets_by_query(
            query,
            tweet_fields,
            results_total=MAX_TWEETS_RETRIEVED,
        )
    else:
        start_date = date - timedelta(days=days_before)
        end_date = date + timedelta(days=days_after)
        tweets = tsu.search_tweets_by_query(
            query,
            tweet_fields=tweet_fields,
            results_total=MAX_TWEETS_RETRIEVED,
            start_time=start_date.strftime("%Y-%m-%d %H:%M"),
            end_time=end_date.strftime("%Y-%m-%d %H:%M"),
        )

    return tweets


def insert_tweets_mongo(tweets, collection):

    for t in tweets:
        # Set the twitter id as the mongo id
        t["_id"] = t["id"]
    print(len(tweets))
    collection.insert_many(tweets, ordered=False)


def main():
    # Iterate through collection
    mydb = mongo_utils.get_mongo_db()
    col_tweets = 'tweets_miscarriage_2022'
    col_tweets = mydb[col_tweets]
    query = config.keywords
    query = " OR ".join(query)
    print(query)
    date = '29/09/2022'
    date = datetime.strptime(date, "%d/%m/%Y")
    days_before = 4
    days_after = 0

    # get only the documents who were not searched for
    tweets = search_twitter(query=query, date=date,
                            days_before=days_before, days_after=days_after)
    insert_tweets_mongo(tweets, col_tweets)


if __name__ == "__main__":
    main()
