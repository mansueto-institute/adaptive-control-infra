FROM python:3

RUN apt-get update --fix-missing && \
    apt-get install -y --allow-unauthenticated git g++ && \
    apt-get clean && \
    pip install git+https://github.com/pymc-devs/pymc3 

COPY app.py ./
COPY main.py ./
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install Flask gunicorn

ENV PORT 8080
EXPOSE 8080

CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 app:app