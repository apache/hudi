---
title: Security
sidebar_position: 5
keywords: [ hudi, security]
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

## Security Model

Apache Hudi is a library that relies on the security posture of the underlying compute engine and 
storage environment in which it operates. In real-world deployments, engines like Presto, Apache Spark, Apache Flink are 
hosted in private virtual network—such as a VPC, VLAN, or on-premises subnet—where only trusted entities have access 
and network controls including firewalls, ACLs, and routing rules are used to restrict and prevent untrusted access. 

## Security Reporting and Vulnerability Handling

The Apache Software Foundation takes security seriously, and Apache Hudi encourages responsible disclosure of any potential 
vulnerabilities. If you have apprehensions regarding Hudi's security, or you discover vulnerability or potential threat, 
don’t hesitate to get in touch with the [Apache Security Team](http://www.apache.org/security/) by dropping a mail at [security@apache.org](mailto:security@apache.org). 
In the mail, specify the description of the issue or potential threat. You are also urged to recommend the way to 
reproduce and replicate the issue. The Hudi community will get back to you after assessing and analysing the findings.

**PLEASE PAY ATTENTION** to report the security issue on the security email before disclosing it on public domain. 

An overview of the vulnerability handling process is:

* The reporter reports the vulnerability privately to Apache.
* The appropriate project's security team works privately with the reporter to resolve the vulnerability.
* A new release of the Apache product concerned is made that includes the fix.
* The vulnerability is publically announced.

A more detailed description of the process can be found [here](https://www.apache.org/security/committers).
