# locis
Implementation of  [A Parallel Spatial Co-location Mining Algorithm Based on MapReduce](docs/paper/paper.pdf) paper

## Setup

* Download and setup Scala, Hadoop (with HDFS) and HBase for versions given [here](docs/implementation.md).
* Copy `src/main/resources/reference.conf.sample` to `src/main/resources/reference.conf` and populate values.
* Run `mvn clean install` in project folder.

### To populate HDFS

* Run `scala -cp target/uber-locis-0.0.1-SNAPSHOT.jar com.github.locis.apps.DataLoader`

### To run Neighbour Search MapReduce task

* Run `$HADOOP_HOME/bin/hadoop jar target/uber-locis-0.0.1-SNAPSHOT.jar com.github.locis.apps.NeighborSearch <input_path> <output_path>`