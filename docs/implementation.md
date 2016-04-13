## Dataset

[Crime data for Chicago city](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2)

## Stack

| Tech   |  Version | 
|--------|----------| 
| Scala  |  2.11.7  | 
| Hadoop |  2.7.2   | 
| HBase  |  1.1.4   | 

## Algorithm

* Input: Data in the form (id, type, (latitude, longitude))
* Map data points to different grids.
* Use plane-sweep algorithm to find neighbors for each data point.
* Count data points for different types.
* Generate size k co-locations (to be expanded.)