<!--
Copyright (c) 2020 YCSB contributors. All rights reserved.

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

## Quick Start

This section describes how to run YCSB on Etcd.

### 1. Start Etcd Server(s)

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    # more details in the landing page for instructions on downloading YCSB(https://github.com/brianfrankcooper/YCSB#getting-started).
    cd YCSB
    mvn -pl site.ycsb:etcd-binding -am clean package -DskipTests

### 4. Provide Etcd Connection Parameters

Set endpoints, eachActionTimeout, namespace, etc. in the workload you plan to run.

- `etcd.endpoints`
  + endpoints of cluster, see [jetcd doc](https://github.com/etcd-io/jetcd#usage) for details
  + if offered in the form of `192.168.56.0:2379`, this binding will automatically wrap `http://` at the head of each endpoint
  + delimited by comma
- `etcd.action.timeout`
  + timeout of each read/insert/update/delete operation
  + timeunit is milliseconds
  + default as `500`
- `etcd.action.preheat`
  + whether to establish session with servers at init stage rather than the 
  first read/insert/update/delete operation
  + default as `true`
- `etcd.action.operator`
  + etcd client api does not support inherit ycsb-update operation
  + this binding offers multiple ycsb-update adaptors:
    + `simpleOperator`: separated read and write
    + `deltaOperator`: write deltas when update
    + `cachedOperator`: if cache hit then write else read-and-retry
    + see [details](#Implementation of YCSB under different operators) for actual operation mapping
    + see [sub-parameters](#Parameters of different operators) for operator parameters
    + if `etcd.action.preheat` is `true`, load all keyValue in etcd to cache
  + default as `cachedOperator`
- `etcd.action.suppressExceptions`
    + debug option
    + if `true`, intercept all exceptions thrown by operator
- `etcd.action.printEach`
    + debug option
    + if `true`, print the result of each operation
- `etcd.charset`
- `etcd.user.name`
- `etcd.user.password`
- `etcd.namespace`
- `etcd.getClusterInfo`
  + whether to get etcd server cluster information on size and endpoints at init stage
  + if set `true`, this option will establish session with servers as well
  + default as `false`

### 5. Load data and run tests

Load the data:

    # -p recordcount,the count of records/paths you want to insert
    ./bin/ycsb load etcd -s -P workloads/workloadb \
        -p etcd.endpoints=192.168.56.1:2379,192.168.56.2:2379,192.168.56.3:2379 \
        -p recordcount=10000 > outputLoad.txt

Run the workload test:

    # YCSB workloadb is the most suitable workload for read-heavy workload for the etcd in the real world.

    # -p fieldlength, test the length of value/data-content took effect on performance
    ./bin/ycsb run etcd -s -P workloads/workloadb \
        -p etcd.endpoints=192.168.56.1:2379,192.168.56.2:2379,192.168.56.3:2379 \
        -p fieldlength=1000

    # -p fieldcount
    ./bin/ycsb run etcd -s -P workloads/workloadb \
        -p etcd.endpoints=192.168.56.1:2379,192.168.56.2:2379,192.168.56.3:2379 \
        -p fieldcount=20

    # -p hdrhistogram.percentiles,show the hdrhistogram benchmark result
    ./bin/ycsb run etcd -threads 1 -P workloads/workloadb \
        -p etcd.endpoints=192.168.56.1:2379,192.168.56.2:2379,192.168.56.3:2379 \
        -p hdrhistogram.percentiles=10,25,50,75,90,95,99,99.9 -p histogram.buckets=500

    ./bin/ycsb run etcd -threads 10 -P workloads/workloadb \
        -p etcd.endpoints=192.168.56.1:2379,192.168.56.2:2379,192.168.56.3:2379

    # show the timeseries benchmark result
    ./bin/ycsb run etcd -threads 1 -P workloads/workloadb \
        -p etcd.endpoints=192.168.56.1:2379,192.168.56.2:2379,192.168.56.3:2379 \
        -p measurementtype=timeseries -p timeseries.granularity=50

### Appendix

#### Implementation of YCSB under different operators

|          | `simpleOperator`  | `deltaOperator` | `cachedOperator`                                           |
|----------|-------------------|-----------------|------------------------------------------------------------|
| `insert` | 1x`Put`           | 1x`Txn`         | 1x`Put`                                                    |
| `read`   | 1x`Get`           | 1x`Txn`         | 1x`Get`                                                    |
| `update` | 1x`Get` + 1x`Put` | 1x`Txn`         | 1x`Txn` (cache hit), 1x`Get` + multiple `Txn` (cache miss) |
| `delete` | 1x`Delete`        | 1x`Txn`         | 1x`Delete`                                                 |
| `scan`   | not implemented   | not implemented | not implemented                                            |

#### Parameters of different operators

- `etcd.deltaOperator.name`
  + each deltaOperator use its name and local counter as a unique tag to each delta
  + if not set, operator will generate a random name
- `etcd.deltaOperator.levelDelimiter`
  + deltaOperator will wrap each key with several prefixes, this option helps combine these prefixes
  + default as `/`
- `etcd.deltaOperator.root`
  + the general prefix of all key operated by deltaOperator
  + must not be confused with keys
  + default as `__ycsb`
- `etcd.deltaOperator.meta`
  + the prefix added to the key meta info
  + default as `meta`
- `etcd.deltaOperator.delta`
  + the prefix added to the key delta info 
  + default as `delta`
- `etcd.cachedOperator.cacheType`
  + cache type
  + default as `hashmap`
- `etcd.cachedOperator.prevKV`
  + if set, try to refresh cache on each insert/read/update/delete
  + if no, only try to refresh cache on each read/update
  + default as `true`
- `etcd.cachedOperator.statOnUpdate`
  + statistics collected during update
  + this helps you select appropriate parameters for this binding
  + the result will write to log during cleanup
  + available for `none`, `cas`, `action`
  + default as `none`
