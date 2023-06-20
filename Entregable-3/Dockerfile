# Dockerfile v1.0

FROM python:3.11

WORKDIR /season-averages

COPY . .

RUN pip install requests psycopg2 pandas

CMD [ "python", "santiago_prates_entregable_3.py" ]