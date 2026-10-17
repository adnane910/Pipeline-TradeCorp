import pytest

#!/usr/bin/env bash
docker exec tradecorp_spark bash -c 'cd /home/jovyan && export PYTHONPATH=src:$SPARK_HOME/python:$(ls $SPARK_HOME/python/lib/py4j-*.zip) && python -m pytest tests/ -v'
