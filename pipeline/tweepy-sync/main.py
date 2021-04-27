import sys
import time

from google.cloud import secretmanager
import tweepy

# cloud details
client = secretmanager.SecretManagerServiceClient()
project_id = "adaptive-control"
#  secret names
secret_names = ["COVID_IWG_twitter_API_key", "COVID_IWG_twitter_secret_key", "Twitter_API_access_token", "Twitter_API_access_secret"]


def fetch_credentials():
    credentials = {}
    for secret_name in secret_names:
        request = {"name": "projects/{}/secrets/{}/versions/latest".format(project_id,secret_name)}
        try:
            response = client.access_secret_version(request)
            credential = response.payload.data.decode("UTF-8")
            credentials[secret_name] = credential
        except Exception as e:
            print("Exception in fetch_credentials!")
            raise e

    return credentials


def obtain_twitter_API():
    credentials = fetch_credentials()
    auth = tweepy.OAuthHandler(credentials["COVID_IWG_twitter_API_key"], credentials["COVID_IWG_twitter_secret_key"])
    auth.set_access_token(credentials["Twitter_API_access_token"], credentials["Twitter_API_access_secret"])
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    print(credentials)
    try:
        api.verify_credentials()
        print("Connected to Twitter API!")
        return api
    except Exception as e:
        print("Cannot connect to Twitter API!")
        raise e
        return None


def tweet_using_API(request):
    api = obtain_twitter_API()
    if not api:
        sys.exit(3)
    status = "Lorem ipsum dolor sit amet, consectetur adipiscing elit at Unix time = {}".format(time.time())
    api.update_status(status)
    return "Tweeted {}".format(status)
