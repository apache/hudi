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
export interface Axis {
  x: number;
  y: number;
}

export interface ContextMenuItem {
  label: string;
  icon?: string;
  hidden?: boolean;
  disabled?: boolean;
  handler?: Fn;
  divider?: boolean;
  children?: ContextMenuItem[];
}
export interface CreateContextOptions {
  event: MouseEvent;
  icon?: string;
  styles?: any;
  items?: ContextMenuItem[];
}

export interface ContextMenuProps {
  event?: MouseEvent;
  styles?: any;
  items: ContextMenuItem[];
  customEvent?: MouseEvent;
  axis?: Axis;
  width?: number;
  showIcon?: boolean;
}

export interface ItemContentProps {
  showIcon: boolean | undefined;
  item: ContextMenuItem;
  handler: Fn;
}
