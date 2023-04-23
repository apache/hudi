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
import type { DescriptionProps, DescInstance, UseDescReturnType } from './typing';
import { ref, getCurrentInstance, unref } from 'vue';
import { isProdMode } from '/@/utils/env';

export function useDescription(props?: Partial<DescriptionProps>): UseDescReturnType {
  if (!getCurrentInstance()) {
    throw new Error('useDescription() can only be used inside setup() or functional components!');
  }
  const desc = ref<Nullable<DescInstance>>(null);
  const loaded = ref(false);

  function register(instance: DescInstance) {
    if (unref(loaded) && isProdMode()) {
      return;
    }
    desc.value = instance;
    props && instance.setDescProps(props);
    loaded.value = true;
  }

  const methods: DescInstance = {
    setDescProps: (descProps: Partial<DescriptionProps>): void => {
      unref(desc)?.setDescProps(descProps);
    },
  };

  return [register, methods];
}
