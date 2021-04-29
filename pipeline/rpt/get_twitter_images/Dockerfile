FROM python:3.9-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Get GDAL dependencies (https://stackoverflow.com/a/46880451)
RUN apt-get update && apt-get install -y \
  binutils \
  gdal-bin \
  python-gdal

# # Create a virtualenv for dependencies. This isolates these packages from system-level packages.
# RUN virtualenv /env -p python3.8

# # Activate virtualenv
# ENV VIRTUAL_ENV /env
# ENV PATH /env/bin:$PATH

# Copy the application's requirements.txt and install all dependencies into the virtualenv.
ADD requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
# Add the application source code.
ENV APP_HOME /app
WORKDIR $APP_HOME
ADD . /app

# Install production dependencies.
RUN pip install Flask gunicorn

# Run the web service on container startup.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app