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
import type { Component } from 'vue';
import {
  Input,
  Select,
  Checkbox,
  InputNumber,
  Switch,
  DatePicker,
  TimePicker,
  AutoComplete,
} from 'ant-design-vue';
import type { ComponentType } from './types/componentType';
import { ApiSelect, ApiTreeSelect } from '/@/components/Form';

const componentMap = new Map<ComponentType, Component>();

componentMap.set('Input', Input);
componentMap.set('InputNumber', InputNumber);
componentMap.set('Select', Select);
componentMap.set('ApiSelect', ApiSelect);
componentMap.set('AutoComplete', AutoComplete);
componentMap.set('ApiTreeSelect', ApiTreeSelect);
componentMap.set('Switch', Switch);
componentMap.set('Checkbox', Checkbox);
componentMap.set('DatePicker', DatePicker);
componentMap.set('TimePicker', TimePicker);

export function add(compName: ComponentType, component: Component) {
  componentMap.set(compName, component);
}

export function del(compName: ComponentType) {
  componentMap.delete(compName);
}

export { componentMap };
