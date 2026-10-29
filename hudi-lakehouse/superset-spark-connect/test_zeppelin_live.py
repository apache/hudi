# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Opt-in Zeppelin notebook test against an isolated Connect endpoint."""
import os
import unittest
import uuid
from urllib.parse import parse_qsl, urlsplit

URL = os.environ.get('HUDI_ZEPPELIN_URL')
CONNECT_URI = os.environ.get('HUDI_ZEPPELIN_CONNECT_URI')


@unittest.skipUnless(URL and CONNECT_URI, 'set HUDI_ZEPPELIN_URL and HUDI_ZEPPELIN_CONNECT_URI')
class ZeppelinLiveTest(unittest.TestCase):
    def test_notebook_crud_and_table_output(self):
        import requests

        parsed = urlsplit(CONNECT_URI)
        options = dict(parse_qsl(parsed.query, keep_blank_values=True))
        allowed = {'api', 'auth_type', 'tls', 'validateservercertificate'}
        if (parsed.scheme != 'spark' or parsed.username or parsed.password
                or options.get('auth_type') != 'none' or options.get('api') != 'connect'
                or set(options) - allowed):
            self.fail('Use an unauthenticated test URI without credentials: it is stored in the notebook')
        base = URL.rstrip('/')
        table = 'zeppelin_test_' + uuid.uuid4().hex
        name = 'Hudi Connect test ' + uuid.uuid4().hex
        # The URI is rendered as a Python string, not interpolated into SQL.
        code = f'''%python
from adbc_driver_manager import dbapi
import pandas as pd
with dbapi.connect(driver="spark", db_kwargs={{"uri": {CONNECT_URI!r}}}, autocommit=True) as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE default.{table} (id INT, region STRING, amount INT, ts BIGINT) USING hudi TBLPROPERTIES(type='cow', primaryKey='id', preCombineField='ts')")
        try:
            cur.execute("INSERT INTO default.{table} VALUES (1,'East',100,1), (2,'West',200,1)")
            cur.execute("SELECT region, SUM(amount) FROM default.{table} GROUP BY region ORDER BY region")
            assert cur.fetchall() == [('East',100),('West',200)]
            cur.execute("UPDATE default.{table} SET amount=150, ts=2 WHERE id=1")
            cur.execute("SELECT id, amount FROM default.{table} WHERE id=1")
            assert cur.fetchall() == [(1,150)]
            cur.execute("MERGE INTO default.{table} t USING (SELECT 1 AS id, 'East' AS region, 175 AS amount, CAST(3 AS BIGINT) AS ts UNION ALL SELECT 3, 'North', 300, CAST(3 AS BIGINT)) s ON t.id=s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
            cur.execute("SELECT id, region, amount, ts FROM default.{table} ORDER BY id")
            assert cur.fetchall() == [(1,'East',175,3),(2,'West',200,1),(3,'North',300,3)]
            cur.execute("DELETE FROM default.{table} WHERE id=2")
            cur.execute("SELECT region, SUM(amount) FROM default.{table} GROUP BY region ORDER BY region")
            rows = cur.fetchall()
            assert rows == [('East',175),('North',300)]
            z.show(pd.DataFrame(rows, columns=['region','total']))
        finally:
            cur.execute("DROP TABLE IF EXISTS default.{table} PURGE")
'''
        response = requests.post(base + '/api/notebook', json={'name': name, 'paragraphs': [{
            'title': 'ADBC Hudi CRUD', 'text': code,
            'config': {'colWidth': 12, 'results': [{'graph': {'mode': 'table'}}]}}]}, timeout=30)
        response.raise_for_status()
        note = response.json()['body']
        succeeded = False
        try:
            response = requests.get(base + '/api/notebook/' + note, timeout=30)
            response.raise_for_status()
            paragraph = response.json()['body']['paragraphs'][0]['id']
            response = requests.post(base + f'/api/notebook/run/{note}/{paragraph}', timeout=240)
            response.raise_for_status()
            result = response.json()['body']
            self.assertEqual(result['code'], 'SUCCESS', result)
            self.assertTrue(any(item['type'] == 'TABLE' and item['data'] == 'region\ttotal\nEast\t175\nNorth\t300\n'
                                for item in result['msg']), result)
            succeeded = True
        finally:
            try:
                response = requests.delete(base + '/api/notebook/' + note, timeout=30)
                response.raise_for_status()
            finally:
                if not succeeded:
                    print(f'Check test table default.{table} after the notebook stops before cleaning it up.')


if __name__ == '__main__':
    unittest.main()
