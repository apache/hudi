# Overview

This folder contains several modules that build out bundles (i.e fat/uber jars) that enable hudi integration into various systems.

Here are the key principles applied in designing these bundles

 - As much as possible, try to make the bundle work with the target system's jars and classes. (e.g: better to make Hudi work with Hive's parquet version than bundling parquet with Hudi). This lets us evolve Hudi as a lighter weight component and also provides flexibility for changing these jar versions in target systems
 - Bundle's pom only needs to depend on the required hudi modules & any other modules that are declared "provided" in parent poms (e.g: parquet-avro). 
 - Such other modules should be declared as "compile" dependency in the bundle pom to actually get the shade plugin in pull them into the bundle. By default, provided scoped dependencies are not included
 - Any other runtime dependencies needed by the bundle should specified in the `<include>` whitelist. New bundles also should follow the same style of explicitly whitelisting modules and shading as needed.
 - Leave abundant comments on why someone is being included, shaded or even being left out.

Please follow these when adding new ones or making changes.

# Resources 

 1. Classes needed for Hive2 JDBC documented [here](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-RunningtheJDBCSampleCode)
 