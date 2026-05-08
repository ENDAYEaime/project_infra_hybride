FROM python:3.10

WORKDIR /app

COPY producer_tickets.py .

RUN pip install kafka-python

CMD ["python", "producer_tickets.py"]