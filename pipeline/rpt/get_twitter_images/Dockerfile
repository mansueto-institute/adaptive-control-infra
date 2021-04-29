FROM osgeo/gdal

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Get GDAL dependencies (https://stackoverflow.com/a/46880451)
RUN apt-get update && apt-get install -y \
  binutils    \
  git         \
  gdal-bin    \
  python3-pip \
  wget        \
  fontconfig

# Install fonts 
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-bold-italic.otf       -O /usr/local/share/fonts/overpass-bold-italic.otf
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-bold.otf              -O /usr/local/share/fonts/overpass-bold.otf
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-extrabold-italic.otf  -O /usr/local/share/fonts/overpass-extrabold-italic.otf
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-extrabold.otf         -O /usr/local/share/fonts/overpass-extrabold.otf
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-extralight-italic.otf -O /usr/local/share/fonts/overpass-extralight-italic.otf
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-extralight.otf        -O /usr/local/share/fonts/overpass-extralight.otf
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-heavy-italic.otf      -O /usr/local/share/fonts/overpass-heavy-italic.otf
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-heavy.otf             -O /usr/local/share/fonts/overpass-heavy.otf
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-italic.otf            -O /usr/local/share/fonts/overpass-italic.otf
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-light-italic.otf      -O /usr/local/share/fonts/overpass-light-italic.otf
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-light.otf             -O /usr/local/share/fonts/overpass-light.otf
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-regular.otf           -O /usr/local/share/fonts/overpass-regular.otf
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-semibold-italic.otf   -O /usr/local/share/fonts/overpass-semibold-italic.otf
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-semibold.otf          -O /usr/local/share/fonts/overpass-semibold.otf
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-thin-italic.otf       -O /usr/local/share/fonts/overpass-thin-italic.otf
RUN wget https://github.com/RedHatOfficial/Overpass/raw/master/desktop-fonts/overpass/overpass-thin.otf              -O /usr/local/share/fonts/overpass-thin.otf
RUN fc-cache -v
RUN fc-match -s overpass

# Copy the application's requirements.txt and install all dependencies into the virtualenv.
ADD pipeline/rpt/get_twitter_images/requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt
# Add the application source code.
ENV APP_HOME /app
WORKDIR $APP_HOME
ADD pipeline/rpt/get_twitter_images /app

# Install production dependencies.
RUN pip3 install Flask gunicorn

# Run the web service on container startup.
# CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1" ,"--threads", "8", "--timeout", "0" ,"main:app"]