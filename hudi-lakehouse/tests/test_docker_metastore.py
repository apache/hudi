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

"""Execute schema recovery with fake Hive commands; no Docker or JVM needed."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'docker/scripts/start-metastore.sh'


class MetastoreBootstrapTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.temp = Path(temporary.name)
        self.database = self.temp / 'metastore_db'
        self.marker = self.temp / '.schema-initialized'
        self.calls = self.temp / 'calls'
        self.bin = self.temp / 'bin'
        self.bin.mkdir()
        self.script = self.temp / 'start.sh'
        # Relocate container paths; exercise the real bootstrap as the hive user.
        self.script.write_text(SCRIPT.read_text()
                               .replace('/opt/hive/data', str(self.temp))
                               .replace('/entrypoint.sh', str(self.bin / 'entrypoint')))
        self.command('id', '#!/bin/sh\nprintf "1000\\n"\n')
        self.command('entrypoint', '#!/bin/sh\nprintf "started\\n" >> "$CALLS"\n')
        self.command('schematool', f'#!{sys.executable}\n' + r'''
import os
from pathlib import Path
import sys
args = sys.argv[1:]
with open(os.environ['CALLS'], 'a') as stream:
    stream.write(' '.join(args) + '\n')
database = Path(os.environ['HIVE_HOME']) / 'metastore_db'
if '-initSchema' in args:
    database.mkdir()
    (database / 'contents').write_text('partial' if os.environ.get('INIT_FAILURE') else 'valid')
    sys.exit(23 if os.environ.get('INIT_FAILURE') else 0)
contents = database / 'contents'
sys.exit(0 if contents.is_file() and contents.read_text() == 'valid' else 29)
''')
        self.env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ['PATH'],
                        HIVE_HOME=str(self.temp), HADOOP_HOME=str(self.temp), CALLS=str(self.calls))
        self.env.pop('INIT_FAILURE', None)

    def command(self, name, source):
        path = self.bin / name
        path.write_text(source)
        path.chmod(0o755)

    def run_bootstrap(self, success=True):
        self.calls.write_text('')
        result = subprocess.run(['bash', str(self.script)], env=self.env,
                                capture_output=True, text=True, timeout=20)
        calls = self.calls.read_text().splitlines()
        self.assertEqual(result.returncode == 0, success, result.stderr)
        self.assertEqual(self.marker.exists(), success)
        self.assertEqual('started' in calls, success)
        if not success:
            self.assertIn('repair or restore', result.stderr)
        return calls

    def test_fresh_database_is_initialized(self):
        self.assertEqual(self.run_bootstrap(), ['-dbType derby -initSchema', 'started'])

    def test_missing_marker_recovers_valid_schema_and_survives_restart(self):
        self.database.mkdir()
        contents = self.database / 'contents'
        contents.write_text('valid')
        self.assertEqual(self.run_bootstrap(), [
            f'-dbType derby -validate -url jdbc:derby:;databaseName={self.database}', 'started'])
        self.assertEqual(contents.read_text(), 'valid')
        self.assertEqual(self.run_bootstrap(), ['started'])

    def test_invalid_database_is_preserved(self):
        for state in ('partial', 'empty', 'dangling-symlink'):
            with self.subTest(state=state):
                if state == 'dangling-symlink':
                    self.database.symlink_to(self.temp / 'absent')
                else:
                    self.database.mkdir()
                    if state == 'partial':
                        (self.database / 'contents').write_text('partial')
                self.assertEqual(self.run_bootstrap(success=False), [
                    f'-dbType derby -validate -url jdbc:derby:;databaseName={self.database}'])
                if state == 'dangling-symlink':
                    self.assertTrue(self.database.is_symlink())
                    self.assertFalse(self.database.exists())
                    self.database.unlink()
                else:
                    if state == 'partial':
                        self.assertEqual((self.database / 'contents').read_text(), 'partial')
                        (self.database / 'contents').unlink()
                    self.assertEqual(list(self.database.iterdir()), [])
                    self.database.rmdir()

    def test_failed_initialization_does_not_reinitialize_partial_data(self):
        self.env['INIT_FAILURE'] = '1'
        self.assertEqual(self.run_bootstrap(success=False), ['-dbType derby -initSchema'])
        self.env.pop('INIT_FAILURE')
        self.assertEqual(self.run_bootstrap(success=False), [
            f'-dbType derby -validate -url jdbc:derby:;databaseName={self.database}'])
        self.assertEqual((self.database / 'contents').read_text(), 'partial')


if __name__ == '__main__':
    unittest.main()
