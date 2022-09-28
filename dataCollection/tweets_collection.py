import os
import sys
from datetime import datetime, timedelta

from mongo_utils import mongo_utils
from twitter_search import TweetSearchUtil

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# for testing purpose limited these are limited now
MAX_TWEETS_RETRIEVED = 500
MAX_CLAIMS_PER_DAY = 1


def search_twitter(query, date=None):
    tsu = TweetSearchUtil("twittercredentials.yaml")
    if date is None:
        tweets = tsu.search_tweets_by_query(
            query,
            results_total=MAX_TWEETS_RETRIEVED,
            tweet_fields="author_id,conversation_id,created_at,geo,id,lang,public_metrics,text",
        )
    else:
        start_date = date + timedelta(days=-30)
        end_date = date + timedelta(days=30)
        tweets = tsu.search_tweets_by_query(
            query,
            results_total=MAX_TWEETS_RETRIEVED,
            tweet_fields="author_id,conversation_id,created_at,geo,id,lang,public_metrics,text",
            start_time=start_date.strftime("%Y-%m-%d %H:%M"),
            end_time=end_date.strftime("%Y-%m-%d %H:%M"),
        )

    return tweets


def insert_tweets_mongo(tweets, source, col_name):
    mydb = mongo_utils.get_mongo_db()
    tweets_col = mydb[col_name]

    for t in tweets:
        # Set the twitter id as the mongo id
        t["_id"] = t["id"]
        t["source"] = source
    print(len(tweets))
    tweets_col.insert_many(tweets)


def main():
    # Iterate through collection
    mydb = mongo_utils.get_mongo_db()
    col_tweets = mydb['tweets_miscarriage_2022']

    # get only the documents who were not searched for
    tweets = search_twitter(query, post_date)
    insert_tweets_mongo(tweets, news_id, col_tweets)


if __name__ == "__main__":
    main()
