import os

import epimargin.plots as plt
import geopandas as gpd
import pandas as pd
from epimargin.etl.covid19india import state_code_lookup
from flask import Flask
from google.cloud import storage

app = Flask(__name__)

CI        = 0.95
smoothing = 10

print("Container starting.")
plt.rebuild_font_cache()
plt.set_theme("twitter")

bucket_name = "daily_pipeline"
bucket = storage.Client().bucket(bucket_name)

@app.route("/")
def hello_world():
    name = os.environ.get("NAME", "World")
    return "Hello {}!".format(name)

@app.route("/state/<state_code>")
def generate_report(state_code: str):
    print(f"Received request for {state_code}.")
    state = state_code_lookup[state_code]
    blobs = { 
        f"pipeline/est/{state_code}_state_Rt.csv"   : "/tmp/state_Rt.csv",
        f"pipeline/est/{state_code}_district_Rt.csv": "/tmp/district_Rt.csv",
        f"pipeline/commons/maps/{state_code}.json"  : "/tmp/state.geojson"
    }
    for (blob_name, filename) in blobs.items():
        bucket.blob(blob_name).download_to_filename(filename)
    print(f"Downloaded estimates for {state_code}.")
    
    state_Rt    = pd.read_csv("/tmp/state_Rt.csv",    parse_dates = ["dates"], index_col = 0)
    district_Rt = pd.read_csv("/tmp/district_Rt.csv", parse_dates = ["dates"], index_col = 0)
    latest_Rt = district_Rt[district_Rt.dates == district_Rt.dates.max()].set_index("district")["Rt_pred"].to_dict()
    top10 = [(k, "> 3.0" if v > 3 else f"{v:.2f}") for (k, v) in sorted(latest_Rt.items(), key = lambda t:t[1], reverse = True)[:10]]

    plt.Rt([date.to_pydatetime() for date in state_Rt.dates], state_Rt.Rt_pred, state_Rt.Rt_CI_lower, state_Rt.Rt_CI_upper, CI)\
        .axis_labels("date", "$R_t$")\
        .title(f"{state}: $R_t$ over time", ha = "center", x = 0.5)\
        .adjust(left = 0.11, bottom = 0.16)
    plt.gcf().set_size_inches(3840/300, 1986/300)
    plt.savefig(f"/tmp/{state_code}_Rt_timeseries.png")
    plt.clf()
    print(f"Generated timeseries plot for {state_code}.")

    gdf = gpd.read_file("/tmp/state.geojson")
    gdf["Rt"] = gdf.district.map(latest_Rt)
    fig, ax = plt.subplots()
    fig.set_size_inches(3840/300, 1986/300)
    plt.choropleth(gdf, title = None, mappable = plt.get_cmap(0.75, 2.5), fig = fig, ax = ax)\
        .adjust(left = 0)
    plt.sca(fig.get_axes()[0])
    plt.PlotDevice(fig).title(f"{state}: $R_t$ by district", ha = "center", x = 0.5)
    plt.axis('off')
    plt.savefig(f"/tmp/{state_code}_Rt_choropleth.png", dpi = 300)
    plt.clf() 
    print(f"Generated choropleth for {state_code}.")

    fig, ax = plt.subplots(1,1)
    ax.axis('tight')
    ax.axis('off')
    table = ax.table(cellText = top10, colLabels = ["district", "$R_t$"], loc = 'center', cellLoc = "center")
    table.scale(1, 2)
    for (row, col), cell in table.get_celld().items():
        if (row == 0):
            cell.set_text_props(fontfamily = plt.theme.label["family"], fontsize = plt.theme.label["size"], fontweight = "semibold")
        else:
            cell.set_text_props(fontfamily = plt.theme.label["family"], fontsize = plt.theme.label["size"], fontweight = "light")
    plt.PlotDevice().title(f"{state}: top districts by $R_t$", ha = "center", x = 0.5)
    plt.savefig(f"/tmp/{state_code}_Rt_top10.png", dpi = 600)
    plt.clf()
    print(f"Generated top 10 district listing for {state_code}.")

    bucket.blob(f"pipeline/rpt/{state_code}_Rt_timeseries.png").upload_from_filename(f"/tmp/{state_code}_Rt_timeseries.png", content_type = "image/png")
    bucket.blob(f"pipeline/rpt/{state_code}_Rt_choropleth.png").upload_from_filename(f"/tmp/{state_code}_Rt_choropleth.png", content_type = "image/png")
    bucket.blob(f"pipeline/rpt/{state_code}_Rt_top10.png")     .upload_from_filename(f"/tmp/{state_code}_Rt_top10.png",      content_type = "image/png")
    print(f"Uploaded artifacts for {state_code}.")
    return "OK!"

if __name__ == "__main__":
    app.run(
        debug = True, 
        host  = "0.0.0.0", 
        port  = int(os.environ.get("PORT", 8080))
    )
