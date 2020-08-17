FROM python:3

RUN apt-get update --fix-missing && \
    apt-get install -y --allow-unauthenticated git g++ && \
    apt-get clean && \
    pip install git+https://github.com/pymc-devs/pymc3 

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
	
COPY simple_pymc3.py ./
CMD python simple_pymc3.py