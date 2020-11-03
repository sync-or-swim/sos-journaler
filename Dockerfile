FROM python:3-alpine

RUN apk add --no-cache g++ libc-dev libxml2-dev libxslt-dev curl
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python - --version 1.1.4
ENV PATH=$PATH:/root/.poetry/bin

WORKDIR /sos-journaler
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false
RUN poetry install --no-root --no-dev

COPY . .

CMD ["python", "main.py"]
