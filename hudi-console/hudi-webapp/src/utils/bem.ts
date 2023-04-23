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
import { prefixCls } from '/@/settings/designSetting';

type Mod = string | { [key: string]: any };
type Mods = Mod | Mod[];

export type BEM = ReturnType<typeof createBEM>;

function genBem(name: string, mods?: Mods): string {
  if (!mods) {
    return '';
  }

  if (typeof mods === 'string') {
    return ` ${name}--${mods}`;
  }

  if (Array.isArray(mods)) {
    return mods.reduce<string>((ret, item) => ret + genBem(name, item), '');
  }

  return Object.keys(mods).reduce((ret, key) => ret + (mods[key] ? genBem(name, key) : ''), '');
}

/**
 * bem helper
 * b() // 'button'
 * b('text') // 'button__text'
 * b({ disabled }) // 'button button--disabled'
 * b('text', { disabled }) // 'button__text button__text--disabled'
 * b(['disabled', 'primary']) // 'button button--disabled button--primary'
 */
export function buildBEM(name: string) {
  return (el?: Mods, mods?: Mods): Mods => {
    if (el && typeof el !== 'string') {
      mods = el;
      el = '';
    }

    el = el ? `${name}__${el}` : name;

    return `${el}${genBem(el, mods)}`;
  };
}

export function createBEM(name: string) {
  return [buildBEM(`${prefixCls}-${name}`)];
}

export function createNamespace(name: string) {
  const prefixedName = `${prefixCls}-${name}`;
  return [prefixedName, buildBEM(prefixedName)] as const;
}
