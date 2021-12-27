Please see `graindb/`, `graphflowdb/`, and `neo4j/` directories for detailed instructions to run experiments related to each system.

### Plotting
#### Boxplot
- End to end benchmarks: JOB, SNB-M, TPC-H
```shell
> python3 scripts/plot_boxplot_job.py evaluations/job.csv
> python3 scripts/plot_boxplot_snb.py evaluations/snb.csv
> python3 scripts/plot_boxplot_tpch.py evaluations/tpch.csv
```

- Ablation
```shell
> python3 scripts/plot_boxplot_ablation.py evaluations/ablation.csv
```

#### Selectivity
- MICRO-P
```shell
> python3 scripts/plot_selectivity.py evaluations/micro-p.csv
```

- MICRO-K
```shell
> python3 scripts/plot_selectivity.py evaluations/micro-k.csv
```

#### Spectrum
```shell
> python3 scripts/plot_spectrum.py evaluations/spectrum_q1.csv -t q1a
> python3 scripts/plot_spectrum.py evaluations/spectrum_q2.csv -t q2a
> python3 scripts/plot_spectrum.py evaluations/spectrum_q3.csv -t q3a
> python3 scripts/plot_spectrum.py evaluations/spectrum_q4.csv -t q4a
> python3 scripts/plot_spectrum.py evaluations/spectrum_q5.csv -t q5a
> python3 scripts/plot_spectrum.py evaluations/spectrum_q6.csv -t q6a
```

### Tips
`screen` or `tmux` are recommemded when running these experiments as some might take very long time.