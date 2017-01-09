---
title: Concepts
keywords: concepts
sidebar: mydoc_sidebar
permalink: concepts.html
---

Hoodie provides the following primitives to build & access datasets on HDFS

 * Upsert                     (how do I change the table efficiently?)
 * Incremental consumption    (how do I obtain records that changed?)


To reason about consistency of the above primitives, Hoodie introduces a notion of `COMMIT`
