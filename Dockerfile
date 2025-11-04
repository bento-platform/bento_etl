FROM ghcr.io/bento-platform/bento_base_image:python-debian-2025.11.01

WORKDIR /etl

COPY pyproject.toml .
COPY poetry.lock .

# Install more recent poetry
# TODO: rm once included in base image
RUN pip install --no-cache-dir poetry==2.2.1

RUN poetry config virtualenvs.create false && \
    poetry --no-cache install --without dev --no-root

COPY bento_etl bento_etl
COPY entrypoint.bash .
COPY run.bash .
COPY LICENSE .
COPY README.md .

RUN poetry install --without dev

ENTRYPOINT [ "bash", "./entrypoint.bash" ]
CMD [ "bash", "./run.bash" ]

