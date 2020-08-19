FROM python:3

# Install G++ and R
RUN apt-get update --fix-missing && \
    apt-get install -y --allow-unauthenticated git g++ && \
	apt-get install -y r-base && \
    apt-get clean

# Copy code into container
COPY *.py ./
COPY cori_model_localstorage.R ./

# Install Python modules for running code and gunicorn
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install gunicorn

# Copy data into new folder
RUN mkdir -p /data
COPY data/covidtracking_cases_clean.csv data/covidtracking_cases_clean.csv

# Install necessary R-packages
RUN Rscript -e "install.packages(c('EpiEstim','tidyr','dplyr','readr','lubridate'))"

# Run on port 8080
ENV PORT 8080
EXPOSE 8080

# On startup, bind app to port 8080 and run
CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 app:app