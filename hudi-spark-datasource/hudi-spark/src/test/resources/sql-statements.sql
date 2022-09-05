# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"), you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# SET OPTION
set hoodie.insert.shuffle.parallelism = 1;
+----------+
| ok       |
+----------+

set hoodie.upsert.shuffle.parallelism = 1;
+----------+
| ok       |
+----------+

set hoodie.delete.shuffle.parallelism = 1;
+----------+
| ok       |
+----------+


# CTAS

create table h0 using hudi options(type = '${tableType}', primaryKey = 'id')
location '${tmpDir}/h0'
as select 1 as id, 'a1' as name, 10 as price;
+----------+
| ok       |
+----------+

select id, name, price from h0;
+-----------+
| 1  a1  10 |
+-----------+

create table h0_p using hudi partitioned by(dt)
options(type = '${tableType}', primaryKey = 'id')
location '${tmpDir}/h0_p'
as select cast('2021-05-07 00:00:00' as timestamp) as dt,
 1 as id, 'a1' as name, 10 as price;
+----------+
| ok       |
+----------+

select id, name, price, cast(dt as string) from h0_p;
+--------------------------------+
| 1  a1  10  2021-05-07 00:00:00 |
+--------------------------------+


# CREATE TABLE

create table h1 (
  id bigint,
  name string,
  price double,
  ts bigint
) using hudi
options (
  type = '${tableType}',
  primaryKey = 'id',
  preCombineField = 'ts'
)
location '${tmpDir}/h1';
+----------+
| ok       |
+----------+

create table h1_p (
  id bigint,
  name string,
  price double,
  ts bigint,
  dt string
) using hudi
partitioned by (dt)
options (
  type = '${tableType}',
  primaryKey = 'id',
  preCombineField = 'ts'
)
location '${tmpDir}/h1_p';
+----------+
| ok       |
+----------+


# INSERT/UPDATE/MERGE/DELETE

insert into h1 values(1, 'a1', 10, 1000);
+----------+
| ok       |
+----------+
insert into h1 values(2, 'a2', 11, 1000);
+----------+
| ok       |
+----------+

# insert static partition
insert into h1_p partition(dt = '2021-05-07') select * from h1;
+----------+
| ok       |
+----------+
select id, name, price, ts, dt from h1_p order by id;
+---------------------------+
| 1 a1 10.0 1000 2021-05-07 |
| 2 a2 11.0 1000 2021-05-07 |
+---------------------------+

# insert overwrite table
insert overwrite table h1_p partition(dt = '2021-05-07') select * from h1 limit 10;
+----------+
| ok       |
+----------+
select id, name, price, ts, dt from h1_p order by id;
+---------------------------+
| 1 a1 10.0 1000 2021-05-07 |
| 2 a2 11.0 1000 2021-05-07 |
+---------------------------+

# insert dynamic partition
insert into h1_p
select id, concat('a', id) as name, price, ts, dt
from (
  select id + 2 as id, price + 2 as price, ts, '2021-05-08' as dt from h1
)
union all
select 5 as id, 'a5' as name, 10 as price, 1000 as ts, '2021-05-08' as dt;
+----------+
| ok       |
+----------+
select id, name, price, ts, dt from h1_p order by id;
+---------------------------+
| 1 a1 10.0 1000 2021-05-07 |
| 2 a2 11.0 1000 2021-05-07 |
| 3 a3 12.0 1000 2021-05-08 |
| 4 a4 13.0 1000 2021-05-08 |
| 5 a5 10.0 1000 2021-05-08 |
+---------------------------+

# update table
update h1_p set price = price * 2 where id % 2 = 1;
+----------+
| ok       |
+----------+
select id, name, price, ts, dt from h1_p order by id;
+---------------------------+
| 1 a1 20.0 1000 2021-05-07 |
| 2 a2 11.0 1000 2021-05-07 |
| 3 a3 24.0 1000 2021-05-08 |
| 4 a4 13.0 1000 2021-05-08 |
| 5 a5 20.0 1000 2021-05-08 |
+---------------------------+

update h1 set price = if (id %2 = 1, price * 2, price);
+----------+
| ok       |
+----------+
select id, name, price, ts from h1;
+----------------+
| 1 a1 20.0 1000 |
| 2 a2 11.0 1000 |
+----------------+

# delete table
delete from h1_p where id = 5;
+----------+
| ok       |
+----------+
select count(1) from h1_p;
+----------+
| 4        |
+----------+

# merge into
merge into h1_p t0
using (
  select *, '2021-05-07' as dt from h1
) s0
on t0.id = s0.id
when matched then update set id = s0.id, name = s0.name, price = s0.price *2, ts = s0.ts, dt = s0.dt
when not matched then insert *;
+----------+
| ok       |
+----------+
select id, name, price, ts, dt from h1_p order by id;
+---------------------------+
| 1 a1 40.0 1000 2021-05-07 |
| 2 a2 22.0 1000 2021-05-07 |
| 3 a3 24.0 1000 2021-05-08 |
| 4 a4 13.0 1000 2021-05-08 |
+---------------------------+

merge into h1_p t0
using (
  select 5 as _id, 'a5' as _name, 10 as _price, 1000 as _ts, '2021-05-08' as dt
) s0
on s0._id = t0.id
when matched then update set *
when not matched then insert (id, name, price, ts, dt) values(_id, _name, _price, _ts, s0.dt);
+----------+
| ok       |
+----------+
select id, name, price, ts, dt from h1_p order by id;
+---------------------------+
| 1 a1 40.0 1000 2021-05-07 |
| 2 a2 22.0 1000 2021-05-07 |
| 3 a3 24.0 1000 2021-05-08 |
| 4 a4 13.0 1000 2021-05-08 |
| 5 a5 10.0 1000 2021-05-08 |
+---------------------------+

merge into h1_p t0
using (
  select 1 as id, '_delete' as name, 10 as price, 1000 as ts, '2021-05-07' as dt
  union
  select 2 as id, '_update' as name, 12 as price, 1001 as ts, '2021-05-07' as dt
  union
  select 6 as id, '_insert' as name, 10 as price, 1000 as ts, '2021-05-08' as dt
) s0
on s0.id = t0.id
when matched and name = '_update'
  then update set id = s0.id, name = s0.name, price = s0.price, ts = s0.ts, dt = s0.dt
when matched and name = '_delete' then delete
when not matched and name = '_insert' then insert *;
+----------+
| ok       |
+----------+
select id, name, price, ts, dt from h1_p order by id;
+--------------------------------+
| 2 _update 12.0 1001 2021-05-07 |
| 3 a3      24.0 1000 2021-05-08 |
| 4 a4      13.0 1000 2021-05-08 |
| 5 a5      10.0 1000 2021-05-08 |
| 6 _insert 10.0 1000 2021-05-08 |
+--------------------------------+

# ALTER TABLE
alter table h1_p rename to h2_p;
+----------+
| ok       |
+----------+
alter table h2_p add columns(ext0 int);
+----------+
| ok       |
+----------+

# DROP TABLE
drop table h0;
+----------+
| ok       |
+----------+

drop table h0_p;
+----------+
| ok       |
+----------+

drop table h1;
+----------+
| ok       |
+----------+

drop table h2_p;
+----------+
| ok       |
+----------+



