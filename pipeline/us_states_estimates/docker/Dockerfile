from sbuschbach/adaptive_control_rpython

# Copy code into container
COPY *.py ./
COPY cori_model.R ./

# Copy data into new folder
# RUN mkdir -p /data
# COPY data/covidtracking_cases_clean.csv data/covidtracking_cases_clean.csv

# Use port 8080
ENV PORT 8080
EXPOSE 8080

# On startup, bind app to port 8080 and run
CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 app:app