FROM ghcr.io/bento-platform/bento_base_image:python-debian-2025.05.05

WORKDIR /etl

COPY pyproject.toml .
COPY poetry.lock .

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

