FROM python:3.8-slim-buster


COPY requirements.txt requirements.txt
# RUN pip3 install -r requirements.txt
RUN pip install -r requirements.txt --index-url https://pypi.python.org/simple
COPY . .
WORKDIR /src
CMD ["python3", "etl_process.py"]