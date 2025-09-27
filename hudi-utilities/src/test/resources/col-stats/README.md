<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

## How To Generate Test
use org.apache.hudi.utilities.testutils.ColStatsUpgradeTesting
to generate the files. It should only need to be updated if we 
change col stats again and need to add another table version to test 
against. Also if we fix logical types for deltastreamer in a 0.15.x 
we should regen the table version 8 with those extra types.

There will be a runscript `runscript.sh` that you can use to download the
bundles and the spark submit deltastreamer job in a loop.
After running the script you will need to zip the directory. The hoodie table
is in the directory as well as schemas, the runscript and .properties files.
For the zip you should cd into the directory like

```
cd /tmp/colstats-upgrade-test-v6
zip -r $HUDI_HOME/hudi-utilities/src/test/resources/col-stats/colstats-upgrade-test-v6.zip .
```
you have to cd into the directory, otherwise when it is unzipped, it will 
maintain the structure of the directory it was zipped from.




