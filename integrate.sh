#!/bin/bash

set -eux

export YUGABYTE_SEEDS=localhost
export YUGABYTE_KEYSPACE=temporal

export ES_SERVER="http://localhost:9200"
export ES_VERSION=v7
export ES_VIS_INDEX=temporal_visibility_v1_dev

start_thirdparty () {
    docker-compose -f docker/docker-compose/integrate.yml up --quiet-pull -d --wait
}

# === Yugabyte functions ===

wait_for_yb() {
    until temporal-cassandra-tool --ep "${YUGABYTE_SEEDS}" validate-health; do
        echo 'Waiting for Yugabyte to start up.'
        sleep 1
    done
    echo 'Yugabyte started.'
}

setup_yb() {
    wait_for_yb

    SCHEMA_DIR=./schema/yugabyte/temporal/versioned
    temporal-cassandra-tool --ep "${YUGABYTE_SEEDS}" create -k "${YUGABYTE_KEYSPACE}" --rf "1"
    temporal-cassandra-tool --ep "${YUGABYTE_SEEDS}" -k "${YUGABYTE_KEYSPACE}" setup-schema -v 0.0
    temporal-cassandra-tool --ep "${YUGABYTE_SEEDS}" -k "${YUGABYTE_KEYSPACE}" update-schema -d "${SCHEMA_DIR}"
}

# === Elasticsearch functions ===

wait_for_es() {

    until curl --silent --fail "${ES_SERVER}" >& /dev/null; do
        echo 'Waiting for Elasticsearch to start up.'
        sleep 1
    done

    echo 'Elasticsearch started.'
}

setup_es() {

    wait_for_es

    SETTINGS_URL="${ES_SERVER}/_cluster/settings"
    SETTINGS_FILE=./schema/elasticsearch/visibility/cluster_settings_${ES_VERSION}.json
    TEMPLATE_URL="${ES_SERVER}/_template/temporal_visibility_v1_template"
    SCHEMA_FILE=./schema/elasticsearch/visibility/index_template_${ES_VERSION}.json
    INDEX_URL="${ES_SERVER}/${ES_VIS_INDEX}"
    curl --fail  -X PUT "${SETTINGS_URL}" -H "Content-Type: application/json" --data-binary "@${SETTINGS_FILE}" --write-out "\n"
    curl --fail -X PUT "${TEMPLATE_URL}" -H 'Content-Type: application/json' --data-binary "@${SCHEMA_FILE}" --write-out "\n"
    curl -X PUT "${INDEX_URL}" --write-out "\n"

}

stop_thirdparty() {
    docker-compose -f docker/docker-compose/integrate.yml down
}

start_thirdparty
setup_yb
setup_es
target/temporal-server --env development --allow-no-auth start
stop_thirdparty
