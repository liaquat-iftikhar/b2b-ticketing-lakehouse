#!/bin/bash
set -e

SPARK_HOME_DETECTED=$(python3 -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))" 2>/dev/null || true)

if [ -n "$SPARK_HOME_DETECTED" ]; then
    export SPARK_HOME="$SPARK_HOME_DETECTED"
    export PATH="${SPARK_HOME}/bin:${PATH}"
    echo "[entrypoint] SPARK_HOME → ${SPARK_HOME}"
else
    echo "[entrypoint] WARNING: pyspark not found — spark-submit will not be available."
fi

exec /entrypoint "$@"
