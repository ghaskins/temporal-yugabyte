#!/bin/bash

set -eux

export YUGABYTE_SEEDS=localhost
export YUGABYTE_KEYSPACE=temporal

export ES_SERVER="http://localhost:9200"
export ES_VERSION=v7
export ES_VIS_INDEX=temporal_visibility_v1_dev

export INTEGRATION_COMPOSE=docker/docker-compose/integrate.yml

export BINDIR=./target

init_yb() {
    until temporal-cassandra-tool --ep "${YUGABYTE_SEEDS}" validate-health; do
        echo 'Waiting for Yugabyte to start up.'
        sleep 1
    done
    echo 'Yugabyte started.'

    SCHEMA_DIR=./schema/yugabyte/temporal/versioned
    temporal-cassandra-tool --ep "${YUGABYTE_SEEDS}" create -k "${YUGABYTE_KEYSPACE}" --rf "1"
    temporal-cassandra-tool --ep "${YUGABYTE_SEEDS}" -k "${YUGABYTE_KEYSPACE}" setup-schema -v 0.0
    temporal-cassandra-tool --ep "${YUGABYTE_SEEDS}" -k "${YUGABYTE_KEYSPACE}" update-schema -d "${SCHEMA_DIR}"
}

init_es() {
    until curl --silent --fail "${ES_SERVER}" >& /dev/null; do
        echo 'Waiting for Elasticsearch to start up.'
        sleep 1
    done

    echo 'Elasticsearch started.'wait_for_es

    SETTINGS_URL="${ES_SERVER}/_cluster/settings"
    SETTINGS_FILE=./schema/elasticsearch/visibility/cluster_settings_${ES_VERSION}.json
    TEMPLATE_URL="${ES_SERVER}/_template/temporal_visibility_v1_template"
    SCHEMA_FILE=./schema/elasticsearch/visibility/index_template_${ES_VERSION}.json
    INDEX_URL="${ES_SERVER}/${ES_VIS_INDEX}"
    curl --fail  -X PUT "${SETTINGS_URL}" -H "Content-Type: application/json" --data-binary "@${SETTINGS_FILE}" --write-out "\n"
    curl --fail -X PUT "${TEMPLATE_URL}" -H 'Content-Type: application/json' --data-binary "@${SCHEMA_FILE}" --write-out "\n"
    curl -X PUT "${INDEX_URL}" --write-out "\n"

}

start_temporal() {
    $BINDIR/temporal-server --env development --allow-no-auth start &
}

wait_for_temporal() {
    until $BINDIR/temporal operator cluster health | grep -q SERVING; do
        echo "Waiting for Temporal server to start..."
        sleep 1
    done
    echo "Temporal server started."
}

start_thirdparty () {
    docker-compose -f $INTEGRATION_COMPOSE up --quiet-pull -d --wait
    init_yb
    init_es
}

stop_thirdparty() {
    docker-compose -f $INTEGRATION_COMPOSE down
}

start_services() {
    start_thirdparty
    start_temporal
}

stop_services() {
    TEMPORAL_PID=$1
    kill -9 $TEMPORAL_PID
    stop_thirdparty
}

start_services

export TEMPORAL_PID=$!
echo "Temporal started on PID: $TEMPORAL_PID"
wait_for_temporal

integration/core/target/core-integration-test -test.v

stop_services $TEMPORAL_PID
