from io import StringIO
import subprocess
import csv
import sys
import re

queries = ['01', '05', '06', '07', '08', '09', '12', '14', '17', '19']

def clean_whitespaces(s):
    return re.sub(r"[ \t\r\f\v]+", "", s)

def run(query_name):
    try:
        exec_path = './build/' + query_name
        result = subprocess.run([exec_path], stdout=subprocess.PIPE)
        perf_data = clean_whitespaces(result.stdout.decode())
    except:
        print("executing" + query_name + "failed")
        sys.exit(1)
    perf_data = '\n'.join(perf_data.splitlines()[1:]) if perf_data.startswith("Error") else perf_data
    df = csv.DictReader(StringIO(perf_data))
    rows = list(df)
    vals = 0.
    for row in rows:
        vals += float(row['millis'])
    return vals / len(rows)


if __name__ == "__main__":
    times = []
    for query in queries:
        times.append(run('tpch-q' + query))
    print(list(map(lambda x : float(x), times)))

