#!/usr/bin/env bash
set -euo pipefail

excluded_commands=(
    blobfuse
    blobfuse2
    docker
    dockerd
    fusermount
    fusermount3
    java
    javac
    livy-server
    mvn
    odbcinst
    pyspark
    sbt
    scala
    spark-submit
)

for excluded_command in "${excluded_commands[@]}"; do
    if command -v "${excluded_command}" >/dev/null 2>&1; then
        echo "Excluded command is present: ${excluded_command}" >&2
        exit 1
    fi
done

excluded_packages="$(
    dpkg-query --show --showformat='${binary:Package}\n' 2>/dev/null \
        | grep -E '^(blobfuse|default-jdk|default-jre|docker|fuse|fuse3|libodbc|livy|maven|moby|odbcinst|openjdk|sbt|scala|spark|unixodbc)(:|$)' \
        || true
)"
if [[ -n "${excluded_packages}" ]]; then
    echo "Excluded packages are present:" >&2
    printf '%s\n' "${excluded_packages}" >&2
    exit 1
fi

echo "Excluded Spark/Java/Scala/Livy/FUSE/ODBC/Docker toolchains are absent."
