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
import { CSSProperties, VNodeChild } from 'vue';
import VueTypes, {
  toType,
  toValidableType,
  createTypes,
  VueTypeValidableDef,
  VueTypesInterface,
} from 'vue-types';

export default class ProjectTypes extends VueTypes {
  // define a custom validator that accepts configuration parameters
  static maxLength() {
    return toType('maxLength', {
      type: String,
      validator: (max, v) => v.length <= max,
    });
  }

  // a native-like validator that supports the `.validable` method
  static get positive() {
    return toValidableType('positive', {
      type: Number,
      validator: (v) => v > 0,
    });
  }
}

export type VueNode = VNodeChild | JSX.Element;

type PropTypes = VueTypesInterface & {
  readonly style: VueTypeValidableDef<CSSProperties>;
  readonly VNodeChild: VueTypeValidableDef<VueNode>;
  // readonly trueBool: VueTypeValidableDef<boolean>;
};

const propTypes = createTypes({
  func: undefined,
  bool: undefined,
  string: undefined,
  number: undefined,
  object: undefined,
  integer: undefined,
}) as PropTypes;

propTypes.extend([
  {
    name: 'style',
    getter: true,
    type: [String, Object],
    default: undefined,
  },
  {
    name: 'VNodeChild',
    getter: true,
    type: undefined,
  },
]);
export { propTypes };
