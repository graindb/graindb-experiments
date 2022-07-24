Experiment evaluations for paper "[Making RDBMSs Efficient on Graph Workloads Through Predefined Joins](https://github.com/graindb/graindb-experiments/blob/master/paper/graindb.pdf)".

Please see `graindb/`, `graphflowdb/`, and `neo4j/` directories for detailed instructions to run experiments related to each system.

### Plotting
#### Boxplot
**End to end benchmarks: JOB, SNB-M, TPC-H.**
- Prepare input csv files:
	- Merge `graindb/evaluations/job_duckdb_avg.out` and `graindb/evaluations/job_graindb_avg.out` into `result/end2end_job.csv`.
	- Merge `graindb/evaluations/snb_duckdb_avg.out`, `graindb/evaluations/mv_duckdb_avg.out`, `graindb/evaluations/snb_graindb_avg.out` and `graphflowdb/evaluations/snb_gfdb_avg.out` into `result/end2end_snb.csv`.
	- Merge `graindb/evaluations/tpch_duckdb_avg.out` and `graindb/evaluations/tpch_graindb_avg.out` into `result/end2end_tpch.csv`.
- Plot the graphs:
```shell
> python3 scripts/plot_boxplot_job.py result/end2end_job.csv
> python3 scripts/plot_boxplot_snb.py result/end2end_snb.csv
> python3 scripts/plot_boxplot_tpch.py result/end2end_tpch.csv
```

**Ablation**
- Merge all perfromance of the ablation study into a single csv file result/ablation.csv.
Each configuration takes a column in the final csv file as the following order: `'DUCKDB', 'GR-JM-RSJ', 'GR-JM', 'GR-FULL'`.
- Plot the graph:
```shell
> python3 scripts/plot_boxplot_ablation.py result/ablation.csv
```

#### Selectivity
- Prepare input csv files:
    - Create an empty file `result/micro_p.csv`. Append "Selectivity" as the first column in the csv file.
    - Append performance columns from `graindb/evaluations/micro_p_duckdb_avg.out`, `graindb/evaluations/micro_p_graindb_avg.out`, `graphflowdb/evaluations/micro_p_gfdb_avg.out`, and `neo4j/micro_p_neo_results.csv` into `result/micro_p.csv`.
    - The final csv file's header is `Selectivity,DuckDB,GRainDB,GFDB,Neo4j`.
    - `result/micro_k.csv` is prepared in a similar way.

- MICRO-P
```shell
> python3 scripts/plot_selectivity.py result/micro_p.csv
```

- MICRO-K
```shell
> python3 scripts/plot_selectivity.py result/micro_k.csv
```

#### Spectrum
- Prepare input csv files:
For each query, organize the performance of DuckDB and GRainDB under different plans in a single csv file.
The header of the csv file is `DuckDB, GRainDB`.
Each row in the csv file corresponds to the performance number of DuckDB and GRainDB under the same join order.

```shell
> python3 scripts/plot_spectrum.py graindb/evaluations/spectrum_q1.csv -t q1a
> python3 scripts/plot_spectrum.py graindb/evaluations/spectrum_q2.csv -t q2a
> python3 scripts/plot_spectrum.py graindb/evaluations/spectrum_q3.csv -t q3a
> python3 scripts/plot_spectrum.py graindb/evaluations/spectrum_q4.csv -t q4a
> python3 scripts/plot_spectrum.py graindb/evaluations/spectrum_q5.csv -t q5a
> python3 scripts/plot_spectrum.py graindb/evaluations/spectrum_q6.csv -t q6a
```


### Tips
`screen` or `tmux` are recommemded when running these experiments as some might take very long time.
