# GrainDB

# Requirements

GrainDB requires [CMake](https://cmake.org) (v 3.21.x) to be installed and a `C++11` compliant compiler (GCC 4.9 and newer, Clang
3.9 and newer).

## Compiling

Run `make` in the root directory to compile the sources. 

# Experiments
## Datasets
- **JOB**: Download all files inside [imdb](https://drive.google.com/drive/folders/1c7e4HfFztGqmBat0wikJXtUI9Cx4CUO7?usp=sharing) to `third_party/imdb/data/`
- **LDBC10**: Download all files inside [ldbc-relational-sf10](https://drive.google.com/drive/folders/106iDhDyCw78nqNJMFPw7ZjHh2XWMe9Pj?usp=sharing) to `third_party/ldbc/data/sf10/`
- **LDBC30**: Download all files inside [ldbc-relational-sf30](https://drive.google.com/drive/folders/1qnpDOXfK_UxmkYDb4aZfSjwSuWrmdAx6?usp=sharing) to `third_party/ldbc/data/sf30/`

## End-To-End Benchmarks

### JOB: Relational Workload with Selective Many-to-Many Joins (8.2.1)
#### Run evaluations of DuckDB and GRainDB on JOB:
```shell
> ./build/release/benchmark/benchmark_runner "IMDB_113_Q[0-9][0-9][0-9]" --out=evaluations/job_duckdb.out
> ./build/release/benchmark/benchmark_runner "IMDB_113_Q[0-9][0-9][0-9]A" --out=evaluations/job_graindb.out
```

#### Processing of query evaluation results:
```shell
> python3 scripts/extract_query_latency.py evaluations/job_duckdb.out evaluations/job_duckdb_avg.out
> python3 scripts/extract_query_latency.py evaluations/job_graindb.out evaluations/job_graindb_avg.out
```

### SNB-M: Graph Workload with Selective Many-to-Many Joins (8.2.2 & 8.2.3)
#### Run evaluations of DuckDB, DuckDB-MV and GRainDB on SNB-M:
```shell
> ./build/release/benchmark/benchmark_runner "LDBC_LIGHT_OPTIMIZED_Q[0-9][0-9][0-9]" --out=evaluations/snb_duckdb.out
> ./build/release/benchmark/benchmark_runner "LDBC_LIGHT_VIEWS_.*" --out=evaluations/mv_duckdb.out
> ./build/release/benchmark/benchmark_runner "LDBC_LIGHT_OPTIMIZED_Q[0-9][0-9][0-9]A" --out=evaluations/snb_graindb.out
```

#### Processing of query evaluation results:
```shell
> python3 scripts/extract_query_latency.py evaluations/snb_duckdb.out evaluations/snb_duckdb_avg.out
> python3 scripts/extract_query_latency.py evaluations/mv_duckdb.out evaluations/mv_duckdb_avg.out
> python3 scripts/extract_query_latency.py evaluations/snb_graindb.out evaluations/snb_graindb_avg.out
```

### TPC-H: Traditional OLAP Workloads (8.2.4)
#### Run TPC-H evaluations on DuckDB and GRainDB:
```shell
> ./build/release/benchmark/benchmark_runner "TPCH_OPTIMIZED_Q[0-9][0-9]" --out=evaluations/tpch_duckdb.out
> ./build/release/benchmark/benchmark_runner "TPCH_OPTIMIZED_Q[0-9][0-9]A" --out=evaluations/tpch_graindb.out
```

#### Processing of query evaluation results:
```shell
> python3 scripts/extract_query_latency.py evaluations/tpch_duckdb.out evaluations/tpch_duckdb_avg.out
> python3 scripts/extract_query_latency.py evaluations/tpch_graindb.out evaluations/tpch_graindb_avg.out
```

## Detailed Evaluation
### Ablation Study (8.3.1)
Configurations for GRainDB:
- GR-FULL. Set both `ENABLE_RAI_JOIN_MERGE` and `ENABLE_ALISTS` to `true`.
- GR-JM. Set `ENABLE_RAI_JOIN_MERGE` to `false`, and keep `ENABLE_ALISTS` to `true`.
- GR-JM-RSJ. Set both `ENABLE_RAI_JOIN_MERGE` and `ENABLE_ALISTS` to `false`.

Under each configuration, re-compile the codebase, and run the following command to get GRainDB evaluation result on SNB-M under that configuration:
```shell
> ./build/release/benchmark/benchmark_runner "LDBC_LIGHT_OPTIMIZED_Q[0-9][0-9][0-9]A" --out=evaluations/snb_graindb_configuration_x.out
```

### Performance of Predefined Joins Under Varying Entity vs Relationship Table Selectivity (8.3.2)
- DuckDB and GRainDB
```shell
> ./build/release/benchmark/benchmark_runner "LDBC_MICROP_Q00[14789]|LDBC_MICROP_Q01[013579]" --out=evaluations/micro_p_duckdb.out
> ./build/release/benchmark/benchmark_runner "LDBC_MICROP_Q00[14789]A|LDBC_MICROP_Q01[013579]A" --out=evaluations/micro_p_graindb.out
> ./build/release/benchmark/benchmark_runner "LDBC_MICROK_Q00[14789]|LDBC_MICROK_Q01[013579]" --out=evaluations/micro_k_duckdb.out
> ./build/release/benchmark/benchmark_runner "LDBC_MICROK_Q00[14789]A|LDBC_MICROK_Q01[013579]A" --out=evaluations/micro_k_graindb.out
```
#### Processing of query evaluation results:
```shell
> python3 scripts/extract_query_latency.py evaluations/micro_p_duckdb.out evaluations/micro_p_duckdb_avg.out
> python3 scripts/extract_query_latency.py evaluations/micro_p_graindb.out evaluations/micro_p_graindb_avg.out
> python3 scripts/extract_query_latency.py evaluations/micro_k_duckdb.out evaluations/micro_k_duckdb_avg.out
> python3 scripts/extract_query_latency.py evaluations/micro_k_graindb.out evaluations/micro_k_graindb_avg.out
```

### Plan Spectrum Analyses (8.3.3)
`spectrum_runner <JOS_INPUT_DIR> <QUERY_ID> <START_JOS_ID> <END_JOS_ID (not included)>`

```shell
> ./build/release/examples/spectrum_runner/spectrum_runner spectrum_jos/Q01/ 1 1 225 > spectrum_q1a.out
> ./build/release/examples/spectrum_runner/spectrum_runner spectrum_jos/Q02/ 5 1 225 > spectrum_q2a.out
> ./build/release/examples/spectrum_runner/spectrum_runner spectrum_jos/Q03/ 9 1 41 > spectrum_q3a.out
> ./build/release/examples/spectrum_runner/spectrum_runner spectrum_jos/Q04/ 12 1 225 > spectrum_q4a.out
> ./build/release/examples/spectrum_runner/spectrum_runner spectrum_jos/Q05/ 15 1 225 > spectrum_q5a.out
> ./build/release/examples/spectrum_runner/spectrum_runner spectrum_jos/Q06/ 18 1 225 > spectrum_q6a.out
```

### Usage
We recommemd users to use the benchmark freamwork for more evaluations.
Take imdb benchmark as an example, its source code is located at `benchmark/imdb/`, and `third_party/imdb` provides data, queries and injected join orders (empty join order file means use DuckDB's default join order optimizer).
Users can follow our examples (imdb, ldbc and tpch) to set up more benchmarks, as this is an easy and efficient way to perform evaluations.
