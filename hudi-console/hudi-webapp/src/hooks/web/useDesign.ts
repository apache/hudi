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
import { useAppProviderContext } from '/@/components/Application';
// import { computed } from 'vue';
// import { lowerFirst } from 'lodash-es';
export function useDesign(scope: string) {
  const values = useAppProviderContext();
  // const $style = cssModule ? useCssModule() : {};

  // const style: Record<string, string> = {};
  // if (cssModule) {
  //   Object.keys($style).forEach((key) => {
  //     // const moduleCls = $style[key];
  //     const k = key.replace(new RegExp(`^${values.prefixCls}-?`, 'ig'), '');
  //     style[lowerFirst(k)] = $style[key];
  //   });
  // }
  return {
    // prefixCls: computed(() => `${values.prefixCls}-${scope}`),
    prefixCls: `${values.prefixCls}-${scope}`,
    prefixVar: values.prefixCls,
    // style,
  };
}
