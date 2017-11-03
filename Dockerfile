FROM python:2.7.13
ADD requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
ADD . /code
WORKDIR /code
