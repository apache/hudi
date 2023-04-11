/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { GLOB_CONFIG_FILE_NAME, OUTPUT_DIR } from '../constant';
import fs, { writeFileSync } from 'fs-extra';
import colors from 'picocolors';

import { getEnvConfig, getRootPath } from '../utils';
import { getConfigFileName } from '../getConfigFileName';

import pkg from '../../package.json';

interface CreateConfigParams {
  configName: string;
  config: any;
  configFileName?: string;
}

function createConfig(params: CreateConfigParams) {
  const { configName, config, configFileName } = params;
  try {
    const windowConf = `window.${configName}`;
    // Ensure that the variable will not be modified
    const configStr = `${windowConf}=${JSON.stringify(config)};
      Object.freeze(${windowConf});
      Object.defineProperty(window, "${configName}", {
        configurable: false,
        writable: false,
      });
    `.replace(/\s/g, '');
    fs.mkdirp(getRootPath(OUTPUT_DIR));
    writeFileSync(getRootPath(`${OUTPUT_DIR}/${configFileName}`), configStr);

    console.log(colors.cyan(`✨ [${pkg.name}]`) + ` - configuration file is build successfully:`);
    console.log(colors.gray(OUTPUT_DIR + '/' + colors.green(configFileName)) + '\n');
  } catch (error) {
    console.log(colors.red('configuration file configuration file failed to package:\n' + error));
  }
}

export function runBuildConfig() {
  const config = getEnvConfig();
  const configFileName = getConfigFileName(config);
  createConfig({ config, configName: configFileName, configFileName: GLOB_CONFIG_FILE_NAME });
}
