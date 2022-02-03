FROM python:2.7
RUN pip install --upgrade pip
ADD requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
ADD . /code
WORKDIR /code
CMD [ "python", "./kafkaOffsetProbe.py" ]
