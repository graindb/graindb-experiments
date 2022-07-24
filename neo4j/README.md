## Requirements
Java 11.

## Setup
- To simulate an in-memory version of Neo4j, you need to first create a ramDisk and perform all following inside the ramdisk.
- Create a dataset directory under current root directory. `mkdir dataset`

## Download Dataset
Download [ldbc-neo-sf30](https://drive.google.com/drive/folders/1JkKFFqUKOuL06fIe-YVsxvkrPtIQAsAa?usp=sharing) and put `v-person.csv` and `e-knows.csv` under `dataset/`

## Install Neo4j
- To download Neo4j community edition `python3 neo4j_setup.py`
  - this will create a `neo4j/` folder that contains the binary of Neo4j 

## Install Neo4j Python Driver
- To install Neo4j python driver `pip3 install neo4j`

## Load Dataset into Neo4j
- To load dataset `python3 neo4j_load.py`

## Start Neo4j
- Start neo4j `./neo4j/neo4j-community-4.3.1/bin/neo4j start`
- Check status `./neo4j/neo4j-community-4.3.1/bin/neo4j status`
- Login to cypher-shell for the first time
  - `./neo4j/neo4j-community-4.3.1/bin/cypher-shell`
  - username: neo4j
  - default password: neo4j
  - new password: 123456 (This is password used in config.py.)
  - to quit `:quit`
- To run micro-benchmark `python3 neo4j_runner`
  - result can be found at `micro_p_neo_avg.csv`
