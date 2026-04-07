import json
import subprocess
import itertools
import pandas as pd
import glob
import os
from typing import Dict
from pathlib import Path

class P2CBenchmark:
    base_dir = "benchmarks"
    perf_base_dir = os.path.join(base_dir, "perf_data")
    csv_base_dir = os.path.join(base_dir, "csv_data")
    flame_base_dir = os.path.join(base_dir, "flamegraph_data")

    # UNCOMMENT DEPENDING ON LOCAL VS AWS
    # AWS PATH LIKELY NEEDS TO BE ADJUSTED TO THE EXACT PERF VERSION AND LOCATIOn
    perf_base = "/usr/lib/linux-tools/6.8.0-101-generic/perf"
    flamegraph_base = "/home/ubuntu/FlameGraph"
    # perf_base = "perf"
    # flamegraph_base = "../FlameGraph"

    perf_command_base = perf_base + " record -g -F 99 --call-graph dwarf"
    necessary_params = [
        "max_network_bandwidth_gb",
        "max_prepared_mb",
        "epoch_duration_ms",
        "warm_up_time_ms",
        "blockpool_size_gb",
        "max_retriever_proportion",
        "ema_alpha",
        "damping_factor",
        "reassignment_threshold"
        ]
    
    cols_to_avg = ["time sec", "millis", "cycles", "kcycles", "instructions", "IPC"]

    def __init__(self, config_template_path: Path, run_perf: bool = False):
        self.run_perf = run_perf
        self.config_template_path = config_template_path
        self.params_dict = {}
        self.queries = []
        self.runs = 1
        self.use_async = True
        
        # Ensure output directories exist
        Path(self.base_dir).mkdir(exist_ok=True)
        Path(self.csv_base_dir).mkdir(exist_ok=True)
        
        if self.run_perf:
            Path(self.perf_base_dir).mkdir(exist_ok=True)
            Path(self.flame_base_dir).mkdir(exist_ok=True)

    
    def read_params(self, params_file: Path):
        try:
            with open(params_file, 'r') as f:
                parsed = json.load(f)
                
                self.queries = parsed.get("queries", [])
                self.runs = parsed.get("runs", 1)
                
                # Extract benchmarking parameters
                for param in self.necessary_params:
                    if param not in parsed:
                        raise KeyError(f"Error: {param} not found in parameter file.")
                    self.params_dict[param] = parsed[param]
                    
        except (json.JSONDecodeError, FileNotFoundError, KeyError) as e:
            print(f"Setup Error: {e}")


    def _update_config(self, params: Dict):
        """Updates the actual config file the query engine reads."""
        with open(self.config_template_path, 'r+') as c_f:
            config_json = json.load(c_f)
            config_json.update(params)
            c_f.seek(0)
            json.dump(config_json, c_f, indent=4)
            c_f.truncate()
    

    def run_benchmarks(self):
        # Generate all combinations of parameters
        keys = self.params_dict.keys()
        values = self.params_dict.values()
        combinations = list(itertools.product(*values))

        # Run baseline (sync) for each query
        for query in self.queries:
            perf_output = os.path.join(self.perf_base_dir, f"{query}_sync_perf.data")
            csv_output = os.path.join(self.csv_base_dir, f"stats_{query}_sync.csv")

            command_parts = [
                f"runs={self.runs}",
                f"use_async=0",
                f"config={self.config_template_path}",
                self.perf_command_base if self.run_perf else "",
                f"-o {perf_output}" if self.run_perf else "",
                f"./build/{query}",
                f"> {csv_output}"
            ]

            full_run_cmd = " ".join(filter(None, command_parts))
            try:
                # Execute query
                subprocess.run(full_run_cmd, shell=True, check=True)
            except subprocess.CalledProcessError as e:
                print(f"Benchmark failed for {query}: {e}")

        # Async queries (actual benchmark):
        for combo in combinations:
            # Create a dict for the current run
            current_params = dict(zip(keys, combo))
            self._update_config(current_params)

            # Create unique filename suffix
            suffix = "_".join([f"{current_params[k]}" for k in self.necessary_params])

            for query in self.queries:
                print(f"Running: {query} with params {suffix}...")
                
                perf_output = os.path.join(self.perf_base_dir, f"{query}_{suffix}_perf.data")
                csv_output = os.path.join(self.csv_base_dir, f"stats_{query}_{suffix}_async.csv")

                # Construct the command
                # env vars before perf
                # -o for perf output
                command_parts = [
                    f"runs={self.runs}",
                    f"use_async=1",
                    f"config={self.config_template_path}",
                    self.perf_command_base if self.run_perf else "",
                    f"-o {perf_output}" if self.run_perf else "",
                    f"./build/{query}",
                    f"> {csv_output}"
                ]

                full_run_cmd = " ".join(filter(None, command_parts))

                try:
                    # Execute query
                    subprocess.run(full_run_cmd, shell=True, check=True)
                    
                except subprocess.CalledProcessError as e:
                    print(f"Benchmark failed for {query}: {e}")

    
    def _generate_flamegraph(self, perf_file: str, output_svg: str):
        try:
            cmd = (
                self.perf_base + f" script -i {perf_file} | "
                f"{self.flamegraph_base}/stackcollapse-perf.pl | "
                f"{self.flamegraph_base}/flamegraph.pl > {output_svg}"
            )
            subprocess.run(cmd, shell=True, check=True)

        except Exception as e:
            print(f"Failed to generate flamegraph for {perf_file}: {e}")

    
    def combine_benchmarks(self):
        num_params = len(self.necessary_params)
        all_data = []
        
        search_path = os.path.join(self.csv_base_dir, "stats_*.csv")
        files = glob.glob(search_path)

        print(f"Combiner: Found {len(files)} benchmark files.")

        for filename in files:
            name_no_ext = os.path.splitext(os.path.basename(filename))[0]
            parts = name_no_ext.split('_')
            
            is_sync = name_no_ext.endswith("_sync")
            
            try:
                df = pd.read_csv(filename, skipinitialspace=True)
                if df.empty: continue
            except Exception: continue

            row = {
                "query": "",
                "variant": df["variant"].iloc[0] if "variant" in df.columns else "unknown",
                "workload": df["workload"].iloc[0] if "workload" in df.columns else "unknown",
            }

            if is_sync:
                row["query"] = "_".join(parts[1:-1])
                for p_name in self.necessary_params:
                    row[p_name] = None
            else:
                params_vals = parts[-(num_params+1):-1]
                row["query"] = "_".join(parts[1:-(num_params+1)])
                for i, p_name in enumerate(self.necessary_params):
                    row[p_name] = params_vals[i]

            for col in self.cols_to_avg:
                if col in df.columns:
                    row[f"avg {col}"] = df[col].mean()

            if "time sec" in df.columns:
                row["time var"] = df["time sec"].var()

            all_data.append(row)

        if not all_data:
            print(f"Combiner: No valid data found.")
            return
        
        full_df = pd.DataFrame(all_data)

        for col in self.necessary_params:
            full_df[col] = pd.to_numeric(full_df[col], errors='coerce')

        unique_queries = full_df["query"].unique()

        for query in unique_queries:
            query_df = full_df[full_df["query"] == query].copy()
            
            # Split data
            baseline_df = query_df[query_df["variant"] == "sync-parquet"]
            async_df = query_df[query_df["variant"] == "async-parquet"].sort_values(by="avg time sec")

            # GENERATE SUMMARY (Baseline on top, then sorted async)
            out_df = pd.concat([baseline_df, async_df])
            out_filename = f"{self.base_dir}/{query}_summary.csv"
            out_df.to_csv(out_filename, index=False)
            print(f"Generated summary: {out_filename}")

            # GENERATE FLAMEGRAPHS (Baseline + Top 5 Async)
            if self.run_perf:
                to_visualize = pd.concat([baseline_df, async_df.head(5)])

                for _, row in to_visualize.iterrows():
                    if row["variant"] == "sync-parquet":
                        perf_filename = f"{row['query']}_sync_perf.data"
                        svg_filename = f"{row['query']}_sync_flame.svg"
                    else:
                        suffix_parts = []
                        for p in self.necessary_params:
                            val = row[p]
                            formatted_val = int(val) if float(val).is_integer() else val
                            suffix_parts.append(str(formatted_val))

                        suffix = "_".join(suffix_parts)
                        perf_filename = f"{row['query']}_{suffix}_perf.data"
                        svg_filename = f"{row['query']}_{suffix}_flame.svg"
                
                    perf_file = os.path.join(self.perf_base_dir, perf_filename)
                    output_svg = os.path.join(self.flame_base_dir, svg_filename)

                    if os.path.exists(perf_file):
                        print(f"Generating FlameGraph: {output_svg}")
                        self._generate_flamegraph(perf_file, output_svg)


########
def main():
    run_perf = os.getenv("RUN_PERF", "false").lower() in ("true", "1", "t")
    bench = P2CBenchmark(config_template_path=Path("config.json"), run_perf=run_perf)
    bench.read_params(Path("config_list.json"))
    bench.run_benchmarks()
    bench.combine_benchmarks()


if __name__ == "__main__":
    main()
