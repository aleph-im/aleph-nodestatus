FROM python:3.9

RUN apt-get update -y && apt-get upgrade -y && apt-get install -y --no-install-recommends apt-utils build-essential cron
ADD ../ /aleph-nodestatus
WORKDIR /aleph-nodestatus
RUN python -m pip install -r requirements.txt
RUN python setup.py install
