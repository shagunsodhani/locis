# locis
Implementation of  [A Parallel Spatial Co-location Mining Algorithm Based on MapReduce](docs/paper/paper.pdf) paper

## Setup

* Download and setup Scala, Hadoop (with HDFS) and HBase for versions given [here](docs/implementation.md).
* Refer [this](https://github.com/shagunsodhani/book-keeper) for sample values for Hadoop and HBase configurations in pseudo distributed mode.
* Copy `src/main/resources/reference.conf.sample` to `src/main/resources/reference.conf` and populate values.
* Run `mvn clean install` in project folder.

### To download dataset

* Obtain an application token from [Socrata portal](https://dev.socrata.com/register) and copy it to `socrata.key` field in `reference.conf`.
* Copy schema from `scripts/schema`.
* Run `python scripts/scrapper/socrata.py`.

### To load data in HDFS

* Run `scala -cp target/uber-locis-0.0.1-SNAPSHOT.jar com.github.locis.apps.DataLoader`

### To run Neighbour Search MapReduce task

* Run `$HADOOP_HOME/bin/hadoop jar target/uber-locis-0.0.1-SNAPSHOT.jar com.github.locis.apps.NeighborSearch <input_path> <output_path>`

### To run Neighbour Grouping MapReduce task

* Run `$HADOOP_HOME/bin/hadoop jar target/uber-locis-0.0.1-SNAPSHOT.jar com.github.locis.apps.NeighborGrouping <input_path> <output_path>`

### To run Count Instance MapReduce task

* Run `$HADOOP_HOME/bin/hadoop jar target/uber-locis-0.0.1-SNAPSHOT.jar com.github.locis.apps.CountInstance <input_path> <output_path>`