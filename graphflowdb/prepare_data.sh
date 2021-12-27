#export JAVA_OPTS="-Xmx100g -Xms100g -XX:+PrintGCDetails -XX:ParallelGCThreads=8"
export JAVA_OPTS="-Xmx80g -Xms80g -XX:ParallelGCThreads=8"

echo "Copy metadata.json"
cp /home/g35jin/ldbc_graph_dataset_generation/graph_datasets/processed_dataset/sf30/metadata.json /home/g35jin/ldbc_dataset/sf1/social_network/graph/metadata.json
cp /home/g35jin/ldbc_graph_dataset_generation/graph_datasets/processed_dataset/sf30/metadata.json /home/g35jin/ldbc_dataset/sf10/social_network/graph/metadata.json
cp /home/g35jin/ldbc_graph_dataset_generation/graph_datasets/processed_dataset/sf30/metadata.json /home/g35jin/ldbc_dataset/sf30/social_network/graph/metadata.json


# R2G
echo "Convert LDBC SF 1"
cd /home/g35jin/graphflow-columnar-techniques/gf-cl/scripts/converter/ && ./snb-convert.sh R2G /home/g35jin/ldbc_dataset/sf1/social_network/static/ /home/g35jin/ldbc_dataset/sf1/social_network/graph/
echo "Serialize LDBC SF 1"
cd /home/g35jin/graphflow-columnar-techniques/gf-cl && python3 scripts/serialize_dataset.py /home/g35jin/ldbc_dataset/sf1/social_network/graph/ /home/g35jin/ldbc_dataset/sf1/social_network/serialized/

echo "Convert LDBC SF 10"
cd /home/g35jin/graphflow-columnar-techniques/gf-cl/scripts/converter/ && ./snb-convert.sh R2G /home/g35jin/ldbc_dataset/sf10/social_network/static/ /home/g35jin/ldbc_dataset/sf10/social_network/graph/
echo "Serialize LDBC SF 10"
cd /home/g35jin/graphflow-columnar-techniques/gf-cl && python3 scripts/serialize_dataset.py /home/g35jin/ldbc_dataset/sf10/social_network/graph/ /home/g35jin/ldbc_dataset/sf10/social_network/serialized/

echo "Convert LDBC SF 30"
cd /home/g35jin/graphflow-columnar-techniques/gf-cl/scripts/converter/ && ./snb-convert.sh R2G /home/g35jin/ldbc_dataset/sf30/social_network/static/ /home/g35jin/ldbc_dataset/sf30/social_network/graph/
echo "Serialize LDBC SF 30"
cd /home/g35jin/graphflow-columnar-techniques/gf-cl && python3 scripts/serialize_dataset.py /home/g35jin/ldbc_dataset/sf30/social_network/graph/ /home/g35jin/ldbc_dataset/sf30/social_network/serialized/


# R2R
echo "Convert LDBC SF 1 to relation"
cd /home/g35jin/graphflow-columnar-techniques/gf-cl/scripts/converter/ && ./snb-convert.sh R2R /home/g35jin/ldbc_dataset/sf1/social_network/static/ /home/g35jin/ldbc_dataset/sf1/social_network/relation/
echo "Convert LDBC SF 10 to relation"
cd /home/g35jin/graphflow-columnar-techniques/gf-cl/scripts/converter/ && ./snb-convert.sh R2R /home/g35jin/ldbc_dataset/sf10/social_network/static/ /home/g35jin/ldbc_dataset/sf10/social_network/relation/
echo "Convert LDBC SF 30 to relation"
cd /home/g35jin/graphflow-columnar-techniques/gf-cl/scripts/converter/ && ./snb-convert.sh R2R /home/g35jin/ldbc_dataset/sf30/social_network/static/ /home/g35jin/ldbc_dataset/sf30/social_network/relation/


# Query Runner
export JAVA_OPTS="-Xmx80g -Xms80g -XX:ParallelGCThreads=8"
export GRAPHFLOW_HOME=`pwd`
./gradlew clean build installDist
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q IC01A -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q IC01B -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q IC08 -w 1 -r 5

./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q IC03A -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q IC05A -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q IC06A -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q IC09A -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q IC11A -w 1 -r 5

./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q IC01 -w 1 -r 5

# Micro query on ldbc10
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-01 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-02 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-03 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-04 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-05 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-06 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-07 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-08 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-09 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-10 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-11 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-12 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-13 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-14 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-15 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-16 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-17 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-18 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf10/social_network/serialized/ -q MICRO1-19 -w 1 -r 5

# Micro query 01 on ldbc30 
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-01 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-02 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-03 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-04 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-05 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-06 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-07 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-08 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-09 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-10 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-11 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-12 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-13 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-14 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-15 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-16 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-17 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-18 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO1-19 -w 1 -r 5

# Micro query 02 on ldbc30 
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-01 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-02 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-03 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-04 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-05 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-06 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-07 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-08 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-09 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-10 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-11 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-12 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-13 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-14 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-15 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-16 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-17 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-18 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO2-19 -w 1 -r 5

# Micro query 03 on ldbc30 
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-01 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-02 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-03 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-04 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-05 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-06 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-07 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-08 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-09 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-10 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-11 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-12 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-13 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-14 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-15 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-16 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-17 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-18 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO3-19 -w 1 -r 5

# Micro query 04 on ldbc30 
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-01 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-02 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-03 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-04 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-05 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-06 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-07 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-08 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-09 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-10 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-11 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-12 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-13 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-14 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-15 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-16 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-17 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-18 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO4-19 -w 1 -r 5

# Micro query 05 on ldbc30 
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-01 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-02 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-03 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-04 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-05 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-06 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-07 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-08 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-09 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-10 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-11 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-12 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-13 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-14 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-15 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-16 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-17 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-18 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO5-19 -w 1 -r 5

# Micro query 08 on ldbc30 
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-01 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-02 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-03 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-04 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-05 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-06 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-07 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-08 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-09 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-10 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-11 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-12 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-13 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-14 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-15 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-16 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-17 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-18 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO8-19 -w 1 -r 5

# Micro query 09 on ldbc30 
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-01 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-02 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-03 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-04 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-05 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-06 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-07 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-08 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-09 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-10 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-11 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-12 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-13 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-14 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-15 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-16 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-17 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-18 -w 1 -r 5
./build/install/graphflow/bin/benchmark-executor -i /home/g35jin/ldbc_dataset/sf30/social_network/serialized/ -q MICRO9-19 -w 1 -r 5
# Local
./build/install/graphflow/bin/benchmark-executor -i ~/Downloads/snb-sf1-serialized/ -q IC01A -w 1 -r 5
python3 scripts/serialize_dataset.py ~/Downloads/snb-sf1-graph/ ~/Downloads/snb-sf1-serialized/
