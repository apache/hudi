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
import type { VNode, CSSProperties } from 'vue';
import type { CollapseContainerOptions } from '/@/components/Container';
import type { DescriptionsProps } from 'ant-design-vue/es/descriptions/index';

export interface DescItem {
  labelMinWidth?: number;
  contentMinWidth?: number;
  labelStyle?: CSSProperties;
  field: string;
  label: string | VNode | JSX.Element;
  // Merge column
  span?: number;
  slot?: string;
  show?: (...arg: any) => boolean;
  // render
  render?: (
    val: any,
    data: Recordable,
  ) => VNode | undefined | JSX.Element | Element | string | number;
}

export interface DescriptionProps extends DescriptionsProps {
  // Whether to include the collapse component
  useCollapse?: boolean;
  /**
   * item configuration
   * @type DescItem
   */
  schema: DescItem[];
  /**
   * data
   * @type object
   */
  data: Recordable;
  /**
   * Built-in CollapseContainer component configuration
   * @type CollapseContainerOptions
   */
  collapseOptions?: CollapseContainerOptions;
}

export interface DescInstance {
  setDescProps(descProps: Partial<DescriptionProps>): void;
}

export type Register = (descInstance: DescInstance) => void;

/**
 * @description:
 */
export type UseDescReturnType = [Register, DescInstance];
