# High Performance Query Processing SS25
This repository is based on the High Performance Query Processing SS25 Project by Lukas Neef

## Repository setup
After cloning the repository, it is necessary to generate the TPC-H data used in the exercises once.
To do this, simply execute the following command:
```
    cd data-generator && ./generate-data.sh
```

## How to use
Make sure that you have installed llvm18 or higher. 
Create a build-directory with `mkdir build` and run `cd build`. To generate the Makefile run `cmake ..`. If llvm18 is not selected by CMake consider recreating the build-directory with `cmake .. -DCMAKE_PREFIX_PATH=/path/to/llvm/cmake`. CMake automatically generates targets for all queries listed below. To generate an executable for a specific query run `make <query-name>`. For example to generate Query 1, run `make tpch-q01`. Simply running `make` compiles all listed queries. 

If you want to inspect the inspected code, make sure to compile the query engine in Debug mode. In this case the generated LLVM-IR is printed to stderr.

The interface of p2cllvm is slightly different than p2c. GroupBy is Aggregation and the basic `Type` enum of p2c has been replaced by a more comprehensive type system. If you want to specify the type, use `TypeEnum::<type>`. The provided qaas-tool is slightly adapted to make up for that. If you want to replace the `AssertTupleSink` which is the standard output sink by an output to stdout, replace `std::make_unique<AssertTupleSink>(expectedValues)` in `planConverter.cpp` with `std::make_unqiue<PrintTupleSink>()`. 

Since the files are built in the build-directory, you have to manually move them to  `queries/definitions/<query>/`. But you can also start them from the build directory. In general the query will be executed with the number of threads equal to `std::hardware_concurrency` .

Before running any query make sure to set the environment variable `tpchpath` to the absolute path to the data set, e.g. `export tpchpath=/opt/tpch-hpqp/sf1/`. Otherwise the engine will be unable to access the dataset. The number of runs is adjusted via the environment variable `runs` and defaults to 3. 

To check the overhead soley caused by the compile time, replace the QueryScheduler in `lib/Driver.cc` with `CompilationTimeScheduler scheduler{db}`.
## Useful links
- [Course on Moodle](https://www.moodle.tum.de/course/view.php?id=106626)
- [Submission and Leaderboard](https://hpqp-leaderboard.dis.cit.tum.de/) Coming soon!
- [Assignment Repository](https://gitlab.lrz.de/hpqp-ss25/tasks-template)

## Supported Queries
1, 5, 6, 7, 8, 9, 12, 14, 17, 19
