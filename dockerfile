FROM python:3.9

RUN mkdir -p /app/datasource
RUN mkdir -p /app/dataloader
WORKDIR /app/dataloader

COPY reqs.txt ./
RUN pip install --no-cache-dir -r reqs.txt
COPY dataloader .

CMD [ "python3", "./main.py" ]