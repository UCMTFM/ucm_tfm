#!/bin/bash

# Ejecuta solo los tests de bronze_layer, gold_layer y silver_layer

set -e
set -x

export PYTHONPATH="$(pwd)/code/src"
export EXECUTION_ENV="databricks-connect"

pytest code/tests/bronze_layer \
       code/tests/gold_layer \
       code/tests/silver_layer \
       -v --maxfail=1 --disable-warnings