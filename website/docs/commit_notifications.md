---
title: Commit Notifications
toc: true
---

Apache Hudi provides the ability to post a callback notification about a write commit. This may be valuable if you need 
an event notification stream to take actions with other services after a Hudi write commit. 
You can push a write commit callback notification into HTTP endpoints or to a Kafka server.

## HTTP Endpoints

