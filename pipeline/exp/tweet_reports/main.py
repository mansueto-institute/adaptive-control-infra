from datetime import date

import tweepy
from google.cloud import secretmanager, storage

# cloud details
project_id = "adaptive-control"
bucket_name = "daily_pipeline"
bucket = storage.Client().bucket(bucket_name)
secrets = secretmanager.SecretManagerServiceClient()

tag_states       = ["MH", "BR", "PB", "TN", "KL"]
dissolved_states = ["Delhi", "Chandigarh", "Manipur", "Sikkim", "Dadra And Nagar Haveli And Daman And Diu", "Andaman And Nicobar Islands", "Telangana", "Goa", "Assam", "Lakshadweep"]
island_states    = ["Puducherry"]

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

def tweet_report(request):
    state_code = get(request, "state_code")
    state = state_code_lookup[state_code]
    normalized_state = state.replace(" and ", " And ")
    print(f"Tweeting report for {state_code} ({state}).")

    blobs = []
    caveats = []

    bucket.blob(f"pipeline/rpt/{state_code}_Rt_timeseries.png").download_to_filename(f"/tmp/{state_code}_Rt_timeseries.png")
    blobs.append(f"/tmp/{state_code}_Rt_timeseries.png")
    
    if normalized_state in (dissolved_states + island_states):
        if normalized_state in dissolved_states:
            caveats.append("calculations run at state-level")
        else: 
            caveats.append("map generation skipped")
    else:
        bucket.blob(f"pipeline/rpt/{state_code}_Rt_choropleth.png").download_to_filename(f"/tmp/{state_code}_Rt_choropleth.png")
        blobs.append(f"/tmp/{state_code}_Rt_choropleth.png")
    
    if state not in dissolved_states:
        bucket.blob(f"pipeline/rpt/{state_code}_Rt_top10.png").download_to_filename(f"/tmp/{state_code}_Rt_top10.png")
        blobs.append(f"/tmp/{state_code}_Rt_top10.png")
    
    hashtag = f"#COVIDmetrics{state_code}"
    tag     = "@anup_malani" if state_code in tag_states else ""
    caveat_text = " (" + ", ".join(caveats) + ") " if caveats else " "

    twitter = get_twitter_client()
    media_ids = [twitter.media_upload(blob).media_id for blob in blobs]
    today = date.today().strftime("%d %b %Y")
    twitter.update_status(
        status    = f"Rt report for {state}, {today}{caveat_text}#covid #Rt #india {hashtag} {tag}", 
        media_ids = media_ids
    )
    return "OK!"
