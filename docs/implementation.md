## Dataset

[Crime data for Chicago city](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2)

## Stack

| Tech   |  Version | 
|--------|----------| 
| Scala  |  2.11.7  | 
| Hadoop |  2.7.2   | 
| HBase  |  1.2.1   | 

## Algorithm

* Input: Data in the form (id, type, (latitude, longitude))
* Map data points to different grids.
* Use plane-sweep algorithm to find neighbors for each data point.
* Perform neighbor grouping.
* Count instances for different types.
* Generate size k co-locations.

## Using HBase Data Model

HBase is used at following places:

* Save *(event, count)* pairs in reducer for [counting instances of different event types](https://github.com/shagunsodhani/locis/issues/8). Here, we can use *event* as the *row key* and *count* as the *value*.

* Save prevalent colocation patterns in reducer for [co-location pattern search](https://github.com/shagunsodhani/locis/issues/7). Here we can use the *eventset* as the *row key*, *size* as the *column key* and *[instance]* as the *value*.

* Read size *k-1* colocations in *scanNTransactions* method in mapper for [co-location pattern search](https://github.com/shagunsodhani/locis/issues/7). Here, the lookup can be performed easily using the *row key* for a given size (*column key*).

## Notes

* The algorithm does not perform candidate set generation.
