---
title: Commit Notifications
toc: true
---

Apache Hudi provides the ability to post a callback notification about a write commit. This may be valuable if you need 
an event notification stream to take actions with other services after a Hudi write commit. 
You can push a write commit callback notification into HTTP endpoints or to a Kafka server.

## HTTP Endpoints
You can push a commit notification to an HTTP URL and can 

**[TODO what is the class_name for?]**

|  Config  | Description | Required | Default |
|  -----------  | -------  | ------- | ------ |
| TURN_CALLBACK_ON | Turn commit callback on/off | optional | false (*callbacks off*) |
| CALLBACK_HTTP_URL | Callback host to be sent along with callback messages | required | N/A |
| CALLBACK_HTTP_TIMEOUT_IN_SECONDS | Callback timeout in seconds | optional | 3 |
| CALLBACK_CLASS_NAME | Full path of callback class and must be a subclass of HoodieWriteCommitCallback class, org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback by default | optional | org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback |
| CALLBACK_HTTP_API_KEY_VALUE | Http callback API key | optional | hudi_write_commit_http_callback |
| | | | |

### Example
**[TODO sample code]**

## Kafka Endpoints
You can push a commit notification to a Kafka topic so it can be used by other real time systems.  

|  Config  | Description | Required | Default |
|  -----------  | -------  | ------- | ------ |
| TOPIC | Kafka topic name to publish timeline activity into. | required | N/A |
| PARTITION | It may be desirable to serialize all changes into a single Kafka partition for providing strict ordering. By default, Kafka messages are keyed by table name, which guarantees ordering at the table level, but not globally (or when new partitions are added) | required | N/A |
| RETRIES | Times to retry the produce | optional | 3 |
| ACKS | kafka acks level, all by default to ensure strong durability | optional | all |
| BOOTSTRAP_SERVERS | Bootstrap servers of kafka cluster, to be used for publishing commit metadata | required | N/A |
| | | | |

### Example
**[TODO sample code]**