Azure Event Hub
============

<a href="https://cdap-users.herokuapp.com/"><img alt="Join CDAP community" src="https://cdap-users.herokuapp.com/badge.svg?t=azure-event-hub"/></a> [![Build Status](https://travis-ci.org/hydrator/azure-event-hub.svg?branch=master)](https://travis-ci.org/hydrator/azure-event-hub) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) <img alt="CDAP Realtime Source" src="https://cdap-users.herokuapp.com/assets/cdap-realtime-source.svg"/> []() <img src="https://cdap-users.herokuapp.com/assets/cm-available.svg"/>

Azure Event Hub streaming source. Emits a record with the schema specified by the user. If no schema
is specified, it will emit a record with 'message'(bytes).

<img align="center" src="azure-event-hub.png"  width="400" alt="plugin configuration" />

Usage Notes
-----------

Azure Event Hub will read events from provided event hub and converts them into structured records so that they can be processed by rest of the CDAP pipeline.
It will use Shared access policy name and key to access that event hub on the azure cluster.

Each event hub can have multiple number of partitions (upto 20). If it is a non-integer value, pipeline deployment will fail.

Since this is a spark streaming source, internally uses Azure Event Hub spark streaming [scala api](https://github.com/hdinsight/spark-eventhubs/blob/master/examples/src/main/scala/com/microsoft/spark/streaming/examples/receiverdstream/workloads/EventhubsEventCount.scala) to read events from all the partitions of event hub.


Plugin Configuration
---------------------

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Azure Event Hub Namespace** | **Y** | N/A | Azure Event Hub namespace under which event hub is present. |
| **Event Hub Name** | **Y** | N/A | Name of the Azure Event Hub under provided namespace. |
| **Shared Access Policy Name** | **Y** | N/A | Name of the policy for the provided event hub. This can be found under `Shared Access Policies` section of the event hub. |
| **Shared Access Policy Key** | **Y** | N/A | Primary key to access the provided event hub. |
| **Number of partitions** | **Y** | N/A | Number of partitions of provided Event Hub. Please make sure this number is right otherwise some messages may not be consumed. |
| **Checkpoint Directory** | **Y** | N/A | HDFS directory location where offsets for each partitions will be stored. |
| **Checkpoint Interval (seconds)** | **N** | 10  | Checkpoint interval in seconds. If not specified, it will default to 10 seconds. |
| **Consumer Group** | **N** | `$default` | Event hub consumer group name, defaults to $default.  |
| **Per Partition Starting Offset** | **N** | -1 | Specify list of partitions for which offset needs to be changed. Defaults to -1 which means all the events in the hub will be read from the beginning.  |
| **Format** | **N** | bytes | Optional format of the event message. Any format supported by CDAP is supported. For example, a value of 'csv' will attempt to parse event as comma-separated values. If no format is given, event will be treated as bytes.  |

Build
-----
To build this plugin:

```
   mvn clean package
```

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/azure-event-hub-<version>.jar config-file <target/azure-event-hub-<version>.json>

For example, if your artifact is named 'azure-event-hub-<version>':

    > load artifact target/azure-event-hub-<version>.jar config-file target/azure-event-hub-<version>.json
    
## Mailing Lists

CDAP User Group and Development Discussions:

* `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.


## License and Trademarks

Copyright © 2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.  