<img src="docs/img/graphflow.png" height="181px" weight="377">

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Build Status](https://travis-ci.com/graphflow/graphflow-core.svg?token=sBsSbpiSo2Uis6z98Ehs&branch=master)](https://travis-ci.org/graphflow/graphflow)
[![Coverage Status](https://coveralls.io/repos/github/graphflow/graphflow-core/badge.svg?branch=master&t=Hv91VR)](https://coveralls.io/github/graphflow/graphflow-core?branch=master)

Build and Setup
-----------------
* Setup:
```shell
export GRAPHFLOW_HOME=`pwd`
export JAVA_OPTS="-Xmx200g -Xms200g -XX:ParallelGCThreads=8
```

* To do a full clean build: `./gradlew clean build installDist`

Dataset
-----------------
* Create directories for dataset:
`mkdir ldbc10 && mkdir ldbc30`
`mkdir ldbc10-serialized && mkdir ldbc30-serialized`

* Download [LDBC10](https://drive.google.com/drive/folders/16RNsnSo2t5pU1Fr-YaRBDaeYAU6dCU7N?usp=sharing) to `ldbc10`.
* Download [LDBC30](https://drive.google.com/drive/folders/10nL4KHqRCyCKn9Xq-nfmss6giulT8zgo?usp=sharing) to `ldbc30`.
* Serialize as
```shell script
python3 script/serailize_dataset.py ldbc10/ ldbc10-serialized/
python3 script/serailize_dataset.py ldbc30/ ldbc30-serialized/
```

Experiments
-----------------

#### SNB-M: Graph Workload with Selective Many-to-Many Joins (8.2.2 & 8.2.3)
Run following commands one at a time, and keep the reported performance number in `evaluations/snb_gfdb_avg.out`.

```shell
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC01A -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC01B -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC01C -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC02 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC03A -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC03B -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC04 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC05A -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC05B -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC06A -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC06B -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC07 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC08 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC09A -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC09B -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC11A -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC11B -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IC12 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IS01 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IS02 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IS03 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IS04 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IS05 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IS06 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc10-serialized/ -q IS07 -w 1 -r 5
```

#### Performance of Predefined Joins Under Varying Entity vs Relationship Table Selectivity (8.3.2)
Run following commands one at a time, and keep the reported performance number in `evaluations/micro_p_gfdb_avg.out`.

- MICROP
```shell
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROP-01 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROP-04 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROP-07 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROP-08 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROP-09 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROP-10 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROP-11 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROP-13 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROP-15 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROP-17 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROP-19 -w 1 -r 5
```

Run following commands one at a time, and keep the reported performance number in `evaluations/micro_k_gfdb_avg.out`.

- MICROK
```shell
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROK-01 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROK-04 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROK-07 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROK-08 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROK-09 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROK-10 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROK-11 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROK-13 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROK-15 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROK-17 -w 1 -r 5
> ./build/install/graphflow/bin/benchmark-executor -i ldbc30-serialized/ -q MICROK-19 -w 1 -r 5
```
