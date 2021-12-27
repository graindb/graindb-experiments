#!/usr/bin/env bash

if [[ -z "${GRAPHFLOW_HOME}" ]]; then
    echo "GRAPHFLOW_HOME is not set."
    echo "Set this environment variable to the root project directory of GF."
    exit 1
fi

# Remove serialized directory if it already exists
if [ -d "${1}/serialized" ]; then 
    rm -rf ${1}/serialized 
fi

echo "Building Graphflow (removed clean, add if needed)"
(cd ${GRAPHFLOW_HOME} && ./gradlew clean build installDist -x test) || exit 1

files=""; vCount=0; eCount=0
for file in ${1}/csv/*.csv; do
    file=$(basename $file)
        IFS='-' read -r -a array <<< "$file"
    name=${array[1]%%.*}
    if [[ $file =~ "e-" ]]; then
	files+=" -Lf${eCount}=${name^^} -Ef${eCount}=${1}/csv/${file}"
        ((eCount++))
    elif [[ $file =~ "v-" ]]; then
        files+=" -Tf${vCount}=${name^^} -Vf${vCount}=${1}/csv/${file}"
        ((vCount++))
    fi
done

echo "Serializing data"
(cd ${GRAPHFLOW_HOME}/scripts && \
python3 serialize_dataset.py "${files}" -o "${1}/serialized") || exit 1

exit 0
