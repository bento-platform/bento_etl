#!/bin/bash

/poetry_user_install_dev.bash

export ASGI_APP="bento_etl.main:app"

: "${INTERNAL_PORT:=5000}"


python -Xfrozen_modules=off -m debugpy --listen 0.0.0.0:9511 -m \
  uvicorn \
  --host 0.0.0.0 \
  --port "${INTERNAL_PORT}" \
  --reload \
  "${ASGI_APP}"
