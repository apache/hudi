---
title: Admin Guide
keywords: admin
sidebar: mydoc_sidebar
permalink: admin_guide.html
---

## Hoodie Admin CLI
### Launching Command Line

<todo - change this after packaging is done>

* mvn clean install in hoodie-cli
* ./hoodie-cli

If all is good you should get a command prompt similar to this one
```
prasanna@:~/hoodie/hoodie-cli$ ./hoodie-cli.sh
16/07/13 21:27:47 INFO xml.XmlBeanDefinitionReader: Loading XML bean definitions from URL [jar:file:/home/prasanna/hoodie/hoodie-cli/target/hoodie-cli-0.1-SNAPSHOT.jar!/META-INF/spring/spring-shell-plugin.xml]
16/07/13 21:27:47 INFO support.GenericApplicationContext: Refreshing org.springframework.context.support.GenericApplicationContext@372688e8: startup date [Wed Jul 13 21:27:47 UTC 2016]; root of context hierarchy
16/07/13 21:27:47 INFO annotation.AutowiredAnnotationBeanPostProcessor: JSR-330 'javax.inject.Inject' annotation found and supported for autowiring
============================================
*                                         \*
*     _    _                 _ _          \*
*    | |  | |               | (_)          *
*    | |__| | ___   ___   __| |_  ___      *
*    |  __  |/ _ \ / _ \ / _` | |/ _ \     *
*    | |  | | (_) | (_) | (_| | |  __/     *
*    |_|  |_|\___/ \___/ \__,_|_|\___|     *
*                                          *
============================================

Welcome to Hoodie CLI. Please type help if you are looking for help.
hoodie->
```

### Commands

 * connect --path [dataset_path]                 : Connect to the specific dataset by its path
 * commits show                                  : Show all details about the commits
 * commits refresh                               : Refresh the commits from HDFS
 * commit rollback      --commit [commitTime]    : Rollback a commit
 * commit showfiles      --commit [commitTime]   : Show details of a commit (lists all the files modified along with other metrics)
 * commit showpartitions --commit [commitTime]   : Show details of a commit (lists statistics aggregated at partition level)

 * commits compare --path [otherBasePath]        : Compares the current dataset commits with the path provided and tells you how many commits behind or ahead
 * stats wa                                      : Calculate commit level and overall write amplification factor (total records written / total records upserted)
 * help



