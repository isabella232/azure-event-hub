# Azure Event Hub Spark Streaming Source


Description
-----------
Azure Event Hub streaming source. Emits a record with the schema specified by the user. If no schema
is specified, it will emit a record with 'message'(bytes).

Use Case
--------
This source is used whenever you want to read from Azure Event Hub. For example, you may want to read messages
from Azure Event Hub and write them to a Table.

Properties
----------
**namespace:** Azure Event Hub namespace under which event hub is present. For example, `event-hub-demo-ns`

**name:** Name of the Azure Event Hub under provided namespace. For example, `event-hub-demo` event hub under `event-hub-demo-ns`

**policyName:** Name of the policy for the provided event hub. This can be found under `Shared Access Policies` section of the event hub.

**policyKey:** Primary key to access the provided event hub.

**partitionCount:** Number of partitions of provided Event Hub. Please make sure this number is right otherwise some messages may not be consumed. 

**checkpointDirectory:** HDFS directory location where offsets for each partitions will be stored.
 
**checkpointInterval:** Checkpoint interval in seconds. If not specified, it will default to 10 seconds.

**consumerGroup:** Event hub consumer group name, defaults to $default.

**offset:** Specify list of partitions for which offset needs to be changed. 
Defaults to -1 which means all the events in the hub will be read from the beginning.

**format** Optional format of the event message. Any format supported by CDAP is supported.
For example, a value of 'csv' will attempt to parse event as comma-separated values.
If no format is given, event will be treated as bytes.

**schema** Output schema of the source. 

Example
-------
This example reads from event hub `event-hub-demo` present under `event-hub-demo-ns` namespace.
It is accessed using Shared Access Primary key `xyz` with name `RootManageSharedAccessKey`.
Now, `event-hub-demo` event but has 2 partitions available. and the checkpoints are stored under `/tmp/eventhub/demo`
directory. The output record is converted into `text` format and field name is `message`.

    {
        "name": "AzureEventHub",
        "type": "streamingsource",
        "properties": {
            "namespace": "event-hub-demo-ns",
            "name": "event-hub-demo",
            "policyName": "RootManageSharedAccessKey",
            "policyKey": "xyz",
            "partitionCount": "2",
            "checkpointDirectory": "/tmp/eventhub/demo",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"etlSchemaBody\",
                \"fields\":[
                    {\"name\":\"message\",\"type\":\"string\"}
                ]
            }",
            "format": "text"
        }
    }