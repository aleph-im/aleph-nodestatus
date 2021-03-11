#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd "$SCRIPT_DIR/.."

# Use Podman if installed, else use Docker
if hash podman 2> /dev/null
then
  DOCKER_COMMAND=podman
else
  DOCKER_COMMAND=docker
fi

#$DOCKER_COMMAND run --rm -ti --name aleph-nodestatus --user aleph \
$DOCKER_COMMAND run --rm -ti --name aleph-nodestatus --user user \
  -v "$(pwd)/src:/opt/aleph_nodestatus/src" \
  alephim/aleph-nodestatus "$@"
