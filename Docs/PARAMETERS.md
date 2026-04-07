# Scheduler Parameters
This short guide goes over the parameters of the `AsyncParquetSchedulder` configuration (specified in `config.json`).

- `blockpool_size_gb` : Total memory capacity allocated to the BlockPool memory pool.
- `max_network_bandwidth_gb` : Physical upper bound of the instance’s network interface (prevent the Scheduler from over-saturating the link).
- `max_prepared_mb` : Cap on total volume of outstanding asynchronous chunk requests.
- `max_retriever_proportion` : Maximum proportion of retriever threads relative to the total number of available threads. *Recommended value*: `0.333`
- `epoch_duration_ms` : Time interval (in milliseconds) between snapshots taken by the main thread to aggregate statistics and set targets. *Recommended value*: `50`
- `warm_up_time_ms` : Configurable delay (in milliseconds) at the start of a query execution before the adaptivity algorithm begins making reassignment decisions. *Recommended value*: `300`
- `damping_factor` (β) : Controls the smoothing of the output target to prevent the Scheduler from overshooting during thread reassignment. *Recommended value*: `0.5`
- `ema_alpha` (α) : Smoothing factor for EMA applied to the performance ratio of processing versus retrieval speed. *Recommended value*: `0.1`
- `reassignment_threshold` (τ) : Deadband value that prevents jittering by requiring a minimum difference before changing the target for retriever threads. *Recommended value*: `0.05`