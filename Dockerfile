FROM python:3.8

COPY . Consumer.py requirements.txt /

RUN pip install -r requirements.txt

CMD ["python", "Consumer.py", "-rq", "https://sqs.us-east-1.amazonaws.com/453835586573/cs5260-requests", "-wt", "widgets"]