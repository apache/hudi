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
import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';

export function isDevFn(mode: string): boolean {
  return mode === 'development';
}

export function isProdFn(mode: string): boolean {
  return mode === 'production';
}

/**
 * Whether to generate package preview
 */
export function isReportMode(): boolean {
  return process.env.REPORT === 'true';
}

// Read all environment variable configuration files to process.env
export function wrapperEnv(envConf: Recordable): ViteEnv {
  const ret: any = {};

  for (const envName of Object.keys(envConf)) {
    let realName = envConf[envName].replace(/\\n/g, '\n');
    realName = realName === 'true' ? true : realName === 'false' ? false : realName;

    if (envName === 'VITE_PORT') {
      realName = Number(realName);
    }
    if (envName === 'VITE_PROXY' && realName) {
      try {
        realName = JSON.parse(realName.replace(/'/g, '"'));
      } catch (error) {
        realName = '';
      }
    }
    ret[envName] = realName;
    if (typeof realName === 'string') {
      process.env[envName] = realName;
    } else if (typeof realName === 'object') {
      process.env[envName] = JSON.stringify(realName);
    }
  }
  return ret;
}

/**
 * Gets the name of the configuration file that is in effect in the current environment
 */
function getConfFiles() {
  const script = process.env.npm_lifecycle_script;
  const reg = new RegExp('--mode ([a-z_\\d]+)');
  const result = reg.exec(script as string) as any;
  if (result) {
    const mode = result[1] as string;
    return ['.env', `.env.${mode}`];
  }
  return ['.env', '.env.production'];
}

/**
 * Get the environment variables starting with the specified prefix
 * @param match prefix
 * @param confFiles ext
 */
export function getEnvConfig(match = 'VITE_GLOB_', confFiles = getConfFiles()) {
  let envConfig = {};
  confFiles.forEach((item) => {
    try {
      const env = dotenv.parse(fs.readFileSync(path.resolve(process.cwd(), item)));
      envConfig = { ...envConfig, ...env };
    } catch (e) {
      console.error(`Error in parsing ${item}`, e);
    }
  });
  const reg = new RegExp(`^(${match})`);
  Object.keys(envConfig).forEach((key) => {
    if (!reg.test(key)) {
      Reflect.deleteProperty(envConfig, key);
    }
  });
  return envConfig;
}

/**
 * Get user root directory
 * @param dir file path
 */
export function getRootPath(...dir: string[]) {
  return path.resolve(process.cwd(), ...dir);
}
