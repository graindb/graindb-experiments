#!/usr/bin/env bash

clean_exit () {
    echo "Wrapping up"

    # Remove temp directory if it exists
    if [ -d "temp" ]; then
        rm -rf temp
    fi

    # Replace modified params.ini with original
    if [ -f "${LDBC_SNB_DATAGEN_HOME}/old-params.ini" ]; then
        mv ${LDBC_SNB_DATAGEN_HOME}/old-params.ini ${LDBC_SNB_DATAGEN_HOME}/params.ini
    fi

    exit $1
}

setup() {
    echo "Setting up"

    # Remove temp directory if it already exists
    if [ -d "temp" ]; then
        rm -rf temp
    fi

    # Make temp directory
    mkdir temp || exit 1

    # Set environment variables
    export HADOOP_CLIENT_OPTS="-Xmx6G"
    export HADOOP_HOME=${LDBC_HOME}/hadoop-2.6.0
    export LDBC_SNB_DATAGEN_HOME=${LDBC_HOME}/ldbc_snb_datagen

    # # Create new params.ini file and replace old file with it
    base="ldbc.snb.datagen.generator.scaleFactor:snb.interactive."
    echo ${base}${1} | cat - ./resources/params.ini > ./temp/params.ini || clean_exit 1
    mv ${LDBC_SNB_DATAGEN_HOME}/params.ini ${LDBC_SNB_DATAGEN_HOME}/old-params.ini || clean_exit 1
    mv ./temp/params.ini ${LDBC_SNB_DATAGEN_HOME}/params.ini || clean_exit 1
}

if [[ $# -lt 2 ]] ; then
    echo 'Two positional arguments required: scaling-factor and output-dir.'
    echo 'Example: ./snb-convert.sh 1.0 ~/output'
    exit 1
fi

if [[ -z "${JAVA_HOME}" ]]; then
    echo "JAVA_HOME is not set."
    exit 1
fi

if [[ -z "${LDBC_HOME}" ]]; then
    echo "LDBC is not set."
    echo "Set this environment variable to the directory containing hadoop and snb."
    exit 1
fi

# Remove csv directory if it already exists
if [ -d "${2}/csv" ]; then
    rm -rf ${2}/csv
fi

setup $1 $2

# Run generator
(cd $LDBC_SNB_DATAGEN_HOME && ./run.sh) || clean_exit 1

echo "Running converter"
rm -rf ${2}/csv
python3 -m snb ${LDBC_SNB_DATAGEN_HOME}/social_network ${2}/csv || clean_exit 1

# todo: run process_dataset.py

# ./snb-serialize.sh $2

clean_exit 0