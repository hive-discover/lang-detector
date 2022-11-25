FROM python:3.7-slim-buster

# Install Dependencies for Fasttext
RUN apt-get update \
    && apt-get install -y gcc python3-dev build-essential \
    && apt-get clean
    
WORKDIR /app

# Install Requirements
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
RUN python3 -m spacy download en_core_web_sm

COPY . .
ENTRYPOINT ["python3"]
CMD ["-u", "entrypoint.py"]
# registry.hive-discover.tech/lang-detector:0.2.1
