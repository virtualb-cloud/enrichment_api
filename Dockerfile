FROM ubuntu:16.04
FROM python:3.7.9


RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-setuptools \
    python-dev

# We copy just the requirements.txt first to leverage Docker cache
COPY ./requirements.txt /app/requirements.txt


WORKDIR /app

RUN pip3 install --no-cache-dir --upgrade pip==22.0.4
RUN pip3 install -r requirements.txt
# RUN pip3 install numpy

COPY . /app

ENTRYPOINT [ "python" ]

CMD [ "main.py" ]


