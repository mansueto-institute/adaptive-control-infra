import tweepy
from google.cloud import secretmanager, storage

# cloud details
project_id = "adaptive-control"
bucket_name = "daily_pipeline"
bucket = storage.Client().bucket(bucket_name)
secrets = secretmanager.SecretManagerServiceClient()

tag_states       = ["MH", "BR", "PB", "TN", "KL"]
dissolved_states = ["Delhi", "Chandigarh", "Manipur", "Sikkim", "Dadra And Nagar Haveli And Daman And Diu", "Andaman And Nicobar Islands", "Telangana", "Goa", "Assam"]
island_states    = ["Lakshadweep", "Puducherry"]

state_code_lookup = {
    'AN'  : 'Andaman & Nicobar Islands',
    'AP'  : 'Andhra Pradesh',
    'AR'  : 'Arunachal Pradesh',
    'AS'  : 'Assam',
    'BR'  : 'Bihar',
    'CH'  : 'Chandigarh',
    'CT'  : 'Chhattisgarh',
    'DD'  : 'Daman & Diu',
    'DDDN': 'Dadra & Nagar Haveli and Daman & Diu',
    'DL'  : 'Delhi',
    'DN'  : 'Dadra & Nagar Haveli',
    'GA'  : 'Goa',
    'GJ'  : 'Gujarat',
    'HP'  : 'Himachal Pradesh',
    'HR'  : 'Haryana',
    'JH'  : 'Jharkhand',
    'JK'  : 'Jammu & Kashmir',
    'KA'  : 'Karnataka',
    'KL'  : 'Kerala',
    'LA'  : 'Ladakh',
    'LD'  : 'Lakshadweep',
    'MH'  : 'Maharashtra',
    'ML'  : 'Meghalaya',
    'MN'  : 'Manipur',
    'MP'  : 'Madhya Pradesh',
    'MZ'  : 'Mizoram',
    'NL'  : 'Nagaland',
    'OR'  : 'Odisha',
    'PB'  : 'Punjab',
    'PY'  : 'Puducherry',
    'RJ'  : 'Rajasthan',
    'SK'  : 'Sikkim',
    'TG'  : 'Telangana',
    'TN'  : 'Tamil Nadu',
    'TR'  : 'Tripura',
    'TT'  : 'India',
    'UN'  : 'State Unassigned',
    'UP'  : 'Uttar Pradesh',
    'UT'  : 'Uttarakhand',
    'WB'  : 'West Bengal',
}
#  secret names
secret_names = ["API_key", "secret_key", "access_token", "access_secret"]

def get(request, key):
    request_json = request.get_json()
    if request.args and key in request.args:
        return request.args.get(key)
    elif request_json and key in request_json:
        return request_json[key]
    else:
        return None


def get_twitter_client(env = "PROD"):
    credentials = {
        secret_name: secrets.access_secret_version({
            "name": f"projects/{project_id}/secrets/{env}_twitter_{secret_name}/versions/latest"}
        ).payload.data.decode("UTF-8")
        for secret_name in secret_names
    }
    auth = tweepy.OAuthHandler(credentials["API_key"], credentials["secret_key"])
    auth.set_access_token(credentials["access_token"], credentials["access_secret"])
    api = tweepy.API(auth, wait_on_rate_limit = True, wait_on_rate_limit_notify = True)
    api.verify_credentials()
    return api


def tweet_vax_report(request):
    state_code = get(request, "state_code")
    state = state_code_lookup[state_code]
    print("Tweeting vaccine report for {} ({}).".format(state_code, state))

    blobs = []

    bucket.blob("pipeline/rpt/first_dose_admin_{}.png".format(state_code)).download_to_filename("/tmp/first_dose_admin_{}.png".format(state_code))
    blobs.append("/tmp/first_dose_admin_{}.png".format(state_code))
    bucket.blob("pipeline/rpt/second_dose_admin_{}.png".format(state_code)).download_to_filename("/tmp/second_dose_admin_{}.png".format(state_code))
    blobs.append("/tmp/second_dose_admin_{}.png".format(state_code))
    bucket.blob("pipeline/rpt/total_individuals_registered_{}.png".format(state_code)).download_to_filename("/tmp/total_individuals_registered_{}.png".format(state_code))
    blobs.append("/tmp/total_individuals_registered_{}.png".format(state_code))


    twitter = get_twitter_client(env="staging")
    media_ids = [twitter.media_upload(blob).media_id for blob in blobs]
    twitter.update_status(
            status="vaccine test plots for {}".format(state),
            media_ids=media_ids
            )
    return "OK!"
