FROM python:3-alpine

RUN pip install --no-cache-dir pipenv

WORKDIR /sos-journaler
COPY Pipfile* ./
RUN pipenv install
COPY . .

ENTRYPOINT ["pipenv", "run", "main"]
