# locis
Implementation of  [A Parallel Spatial Co-location Mining Algorithm Based on MapReduce](docs/paper/paper.pdf) paper

## Setup

* Download and setup Scala, Hadoop (with HDFS) and HBase for versions given [here](docs/implementation.md).
* Refer [this](https://github.com/shagunsodhani/book-keeper) for sample values for Hadoop and HBase configurations in pseudo distributed mode and [this](docs/known-issues.md) for some known issues when setting up HBase. 
* Start Hadoop using `$HADOOP_HOME/sbin/start-dfs.sh` and HBase using `$HBASE_HOME/bin/start-hbase.sh`.
* Verify that Hadoop and HBase are working propery by opening [http://localhost:50070/](http://localhost:50070/) and [http://localhost:16010/](http://localhost:16010/) respectively.
* Copy `src/main/resources/reference.conf.sample` to `src/main/resources/reference.conf` and populate values.
* Run `mvn clean install` in project folder.

### To download dataset

* Obtain an application token from [Socrata portal](https://dev.socrata.com/register) and copy it to `socrata.key` field in `reference.conf`.
* Copy schema from `scripts/schema`.
* Run `python scripts/scrapper/socrata.py`.

### To load data in HDFS

* Run `scala -cp target/uber-locis-0.0.1-SNAPSHOT.jar com.github.locis.apps.DataLoader <input_path_to_write_raw_data>`
* If no path is provided, it writes to `/user/locis/input/data`

### Dummy Dataset

* A very small dataset (6 rows) can be found in `sampleData\data` file. The file can be used for testing the different MapReduce tasks without having to download the socrata dataset. 
*Add the file to hdfs using the put command `$HADOOP_HOME/bin/hdfs dfs -put <path_to_locis>/sampleData/data <input_path_to_write_raw_data>` and proceed to run MapReduce tasks.

### To run Neighbour Search MapReduce task

* Run `$HADOOP_HOME/bin/hadoop jar target/uber-locis-0.0.1-SNAPSHOT.jar com.github.locis.apps.NeighborSearch <input_path_to_read_raw_data> <output_path_to_write_neighbors>`

### To run Neighbour Grouping MapReduce task

* Run `$HADOOP_HOME/bin/hadoop jar target/uber-locis-0.0.1-SNAPSHOT.jar com.github.locis.apps.NeighborGrouping <input_path_to_read_neighbors> <output_path_to_write_neighbor_groups>`

### To run Count Instance MapReduce task

* Run `$HADOOP_HOME/bin/hadoop jar target/uber-locis-0.0.1-SNAPSHOT.jar com.github.locis.apps.CountInstance <input_path_to_read_neighbor_groups> <output_path_to_write_instance_count>`

### To run Colocation Pattern Search MapReduce task

* Run `$HADOOP_HOME/bin/hadoop jar target/uber-locis-0.0.1-SNAPSHOT.jar com.github.locis.apps.PatternSearch <input_path_to_read_neighbor_groups> <output_path_to_write_prevalence_scores> <size_of_colocation>`

Note that for running colocation pattern search task for size k, the results for size 1 to *k-1* should already be in the db. So to find colocation patterns of size *k*, run the script for 1 to *k* and not just *k*. This task can be easily automated using a bash script. 