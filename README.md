<!--
Copyright (c) 2010 Yahoo! Inc., 2012 - 2016 YCSB contributors.
Copyright (c) 2023 - 2024 benchANT GmbH.
All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

About
====================================

This is a modification of the Yahoo! Cloud Serving Benchmark (YCSB) that brings a new workload and a set of new workload-related features for the following five different database bindings

1. MongoDB
2. Scylla
3. JDBC
4. DynamoDB
5. Couchbase3

### Workload

The new workload is built around a custom document structure that also comprises nested documents. 

```
{
	"airline":{
		"name": str, pre-generated 50 unique values,
		"alias": str, align with name
	},
	"src_airport": str, pre-generated 500 unique values,
	"dst_airport": str, pre-generated 500 unique values,
	"codeshares": str array, length 0 to 3, from airline aliases
	"stops": int, from 0 to 3
	"airplane": str, pre-generated 10 unique values,
	"field1": str, random length from 0 to 1000
}
```

The workload can be enabled by setting `workload=site.ycsb.workloads.airport.AirportWorkload`. It makes use of four different types of operations:

* insert a single document
* insert bulks of documents
* delete a document by primary key
* query any document by `src_airport` (equality), `dst_airport`  (equality), and `stops` (lower or equal)
* update one document by `airline.alias`

### Binding

In order to support this workload, the implemented bindings support two new functions compared to default YCSB: `findOne` and `updateOne`. Further, all bindings have been updated to support bulk inserts and delete by primary key.

While YCSB only uses stringified content, the new workload requires the use of more diverse data types.  Therefore the property`typedfields=true` needs to be set. Also, the bindings have been updated to support and distinguish between different data types. What is more,  bindings for document-oriented DBMS are built such that they handle nested documents. Other bindings, in particular for JDBC and ScyllaDB are designed such that they use a flattened data model. Where possible, the updated bindings also support the definition of indexes. The full feature list is shown in the following table.

|            | **typed content** | **support for indexes** | **nested elements** |
| ---------- | ----------------- | ----------------------- | ------------------- |
| mongodb    | yes               | full                    | yes                 |
| jdbc       | yes               | full                    | no                  |
| couchbase3 | yes               | full                    | yes                 |
| dynamodb   | yes               | single column           | yes                 |
| scylla     | yes               | single column           | no                  |

The workload class can be configured to either use nested elements or use a flat data model.

```
nesteddata = false | true
```

Other changes this branch makes compared to original YCSB:

* Full support for `long` (64 bits) ids so that more data can be added to the databases in the LOAD phase
* A new Id generator that does not use a window of in-transit ids and does not crash when large amounts of data are inserted in the database. It can be enabled with  `transactioninsertkeygenerator=simple`

In order to support queries all bindings now come with basic support for indexes. These can be set with a binding-specific flag:

```json
couchbase.indexlist=[ ]
dynamodb.indexlist=[ ]
jdbc.indexlist=[ ]
mongodb.indexlist=[ ]
scylla.indexlist=[ ]
```

Example indexes are available in the `workloads/airport ` directory.

YCSB
====================================

[![Build Status](https://travis-ci.org/brianfrankcooper/YCSB.png?branch=master)](https://travis-ci.org/brianfrankcooper/YCSB)



Links
-----
* To get here, use https://ycsb.site
* [Our project docs](https://github.com/brianfrankcooper/YCSB/wiki)
* [The original announcement from Yahoo!](https://labs.yahoo.com/news/yahoo-cloud-serving-benchmark/)

Getting Started
---------------

1. Download the [latest release of YCSB](https://github.com/brianfrankcooper/YCSB/releases/latest):

    ```sh
    curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
    tar xfvz ycsb-0.17.0.tar.gz
    cd ycsb-0.17.0
    ```
    
2. Set up a database to benchmark. There is a README file under each binding 
   directory.

3. Run YCSB command. 

    On Linux:
    ```sh
    bin/ycsb.sh load basic -P workloads/workloada
    bin/ycsb.sh run basic -P workloads/workloada
    ```

    On Windows:
    ```bat
    bin/ycsb.bat load basic -P workloads\workloada
    bin/ycsb.bat run basic -P workloads\workloada
    ```

  Running the `ycsb` command without any argument will print the usage. 

  See https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload
  for a detailed documentation on how to run a workload.

  See https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for 
  the list of available workload properties.


Building from source
--------------------

YCSB requires the use of Maven 3; if you use Maven 2, you may see [errors
such as these](https://github.com/brianfrankcooper/YCSB/issues/406).

To build the full distribution, with all database bindings:

    mvn clean package

To build a single database binding:

    mvn -pl site.ycsb:mongodb-binding -am clean package
