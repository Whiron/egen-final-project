# syntax=docker/dockerfile:1
FROM python:3.7
COPY covid-data-project-328321-c8cad2a05e5f.json covid-data-project-328321-c8cad2a05e5f.json
COPY covid-data-project-328321-c37b3c4360fb.json covid-data-project-328321-c37b3c4360fb.json
COPY pub_docker.py pub_docker.py
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
CMD [ "python3", "pub_docker.py"]