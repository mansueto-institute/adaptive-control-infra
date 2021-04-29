FROM osgeo/gdal

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Get GDAL dependencies (https://stackoverflow.com/a/46880451)
RUN apt-get update && apt-get install -y \
  binutils    \
  git         \
  gdal-bin    \
  python3-pip

# Copy the application's requirements.txt and install all dependencies into the virtualenv.
ADD pipeline/rpt/get_twitter_images/requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
# Add the application source code.
ENV APP_HOME /app
WORKDIR $APP_HOME
ADD pipeline/rpt/get_twitter_images /app

# Install production dependencies.
RUN pip install Flask gunicorn

# Run the web service on container startup.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app