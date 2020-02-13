FROM python:3-alpine

RUN apk add --no-cache g++ libc-dev libxml2-dev libxslt-dev
RUN pip install --no-cache-dir pipenv

WORKDIR /sos-journaler
COPY Pipfile* ./
RUN pipenv install --deploy --system --ignore-pipfile
COPY . .

ENTRYPOINT ["python", "main.py"]
