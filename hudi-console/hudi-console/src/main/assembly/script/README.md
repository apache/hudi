# Hudi Console DataBase Guide

## Initialization Guide

Currently, Hudi console supports MySQL and PostgreSQL. The schema directory is the current database schema, it includes mysql and pgsql
schema. The data directory is the current complete data, it includes mysql and pgsql data.

If you use MySQL as the Hudi database, please execute `mysql-schema.sql` first, then execute `mysql-data.sql` to initialize data.

If you use PostgreSQL as the Hudi database, please execute `pgsql-schema.sql` first, then execute `pgsql-data.sql` to initialize data.

## Upgrade Guide

When upgrading HUDI Console from an old version to a new version, you need to execute some SQL to upgrade the schema and data of the database.

We sorted out the upgrade sql in the upgrade directory, you can choose `mysql` or `pgsql` according to your database. And we have sorted out
the corresponding upgrade SQL for each version of HUDI-Console. The `version.sql` means: upgrade from the previous version to the current
version, and it includes schema changes and data changes.

For example:

- `1.2.3.sql` needs to be executed when HUDI-Console is upgraded from `1.2.2` to `1.2.3`.
- `1.2.3.sql` and `2.0.0.sql`  needs to be executed when HUDI-Console is upgraded from `1.2.2` to `2.0.0`. 
