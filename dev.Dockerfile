FROM ghcr.io/bento-platform/bento_base_image:python-debian-2026.01.14

LABEL org.opencontainers.image.description="Local development image for the Bento ETL service."
LABEL devcontainer.metadata='[{ \
  "remoteUser": "bento_user", \
  "customizations": { \
    "vscode": { \
      "extensions": ["ms-python.python", "eamodio.gitlens"], \
      "settings": {"workspaceFolder": "/etl"} \
    } \
  } \
}]'

WORKDIR /etl

COPY pyproject.toml .
COPY poetry.lock .

COPY run.dev.bash .

# Install more recent poetry
# TODO: rm once included in base image
RUN pip install --no-cache-dir poetry==2.1.3

RUN poetry config virtualenvs.create false && \
    poetry --no-cache install --no-root

ENV BENTO_CONTAINER_LOCAL=true

CMD [ "bash", "./run.dev.bash" ]
