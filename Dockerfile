FROM python:3.13-slim 

ENV PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y \
    && apt-get install -y git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip uv

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-warn-script-location -r /tmp/requirements.txt

COPY . /tmp

RUN pip install --no-cache-dir -e /tmp

WORKDIR /srv

CMD ["bash"]
