#!/usr/bin/env bash

clean_exit () {
    echo "Wrapping up"
}

setup() {
    echo "Setting up "
    echo "Type: "$1
    echo "Input: "$2
    echo "Output: "$3
}

if [[ $# -lt 3 ]] ; then
    echo 'Three positional arguments required: type input-dir and output-dir.'
    echo 'Example: ./snb-convert.sh R2G ~/input ~/output'
    exit 1
fi

setup $1 $2

echo "Running converter"
python3 -m snb ${1} ${2} ${3} || clean_exit 1

clean_exit 0

# R2G /home/g35jin/ldbc_dataset/sf1/social_network/static /home/g35jin/ldbc_dataset/sf1/social_network/graph
# R2G /Users/guodong/Downloads/snb-sf1-relation/ /Users/guodong/Downloads/snb-sf1-graph/
