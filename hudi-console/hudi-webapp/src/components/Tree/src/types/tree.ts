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
import type { ExtractPropTypes } from 'vue';
import type { TreeDataItem } from 'ant-design-vue/es/tree/Tree';

import { buildProps } from '/@/utils/props';

export enum ToolbarEnum {
  SELECT_ALL,
  UN_SELECT_ALL,
  EXPAND_ALL,
  UN_EXPAND_ALL,
  CHECK_STRICTLY,
  CHECK_UN_STRICTLY,
}

export const treeEmits = [
  'update:expandedKeys',
  'update:selectedKeys',
  'update:value',
  'change',
  'check',
  'update:searchValue',
];

export interface TreeState {
  expandedKeys: KeyType[];
  selectedKeys: KeyType[];
  checkedKeys: CheckKeys;
  checkStrictly: boolean;
}

export interface FieldNames {
  children?: string;
  title?: string;
  key?: string;
}

export type KeyType = string | number;

export type CheckKeys =
  | KeyType[]
  | { checked: string[] | number[]; halfChecked: string[] | number[] };

export const treeProps = buildProps({
  value: {
    type: [Object, Array] as PropType<KeyType[] | CheckKeys>,
  },

  renderIcon: {
    type: Function as PropType<(params: Recordable) => string>,
  },

  helpMessage: {
    type: [String, Array] as PropType<string | string[]>,
    default: '',
  },

  title: {
    type: String,
    default: '',
  },
  toolbar: Boolean,
  search: Boolean,
  searchValue: {
    type: String,
    default: '',
  },
  checkStrictly: Boolean,
  clickRowToExpand: {
    type: Boolean,
    default: false,
  },
  checkable: Boolean,
  defaultExpandLevel: {
    type: [String, Number] as PropType<string | number>,
    default: '',
  },
  defaultExpandAll: Boolean,

  fieldNames: {
    type: Object as PropType<FieldNames>,
  },

  treeData: {
    type: Array as PropType<TreeDataItem[]>,
  },

  actionList: {
    type: Array as PropType<TreeActionItem[]>,
    default: () => [],
  },

  expandedKeys: {
    type: Array as PropType<KeyType[]>,
    default: () => [],
  },

  selectedKeys: {
    type: Array as PropType<KeyType[]>,
    default: () => [],
  },

  checkedKeys: {
    type: Array as PropType<CheckKeys>,
    default: () => [],
  },

  beforeRightClick: {
    type: Function as PropType<(...arg: any) => ContextMenuItem[] | ContextMenuOptions>,
    default: undefined,
  },

  rightMenuList: {
    type: Array as PropType<ContextMenuItem[]>,
  },
  // Custom data filtering judgment method (Note: not the entire filtering method, but the built-in filtering judgment method, which is used to enhance the method that can only be filtered by title)
  filterFn: {
    type: Function as PropType<
      (searchValue: any, node: TreeItem, fieldNames: FieldNames) => boolean
    >,
    default: undefined,
  },
  //Highlight the search value, only highlight the default color value when the specific matching value (via title) value is true, and use this value instead when the value is #xxx and highlight it on
  highlight: {
    type: [Boolean, String] as PropType<Boolean | String>,
    default: false,
  },
  // Automatically expand the results when the search is complete
  expandOnSearch: Boolean,
  // Search completion automatically selects all results if and only if checkable===true
  checkOnSearch: Boolean,
  // Search complete automatically select all results
  selectedOnSearch: Boolean,
  loading: {
    type: Boolean,
    default: false,
  },
});

export type TreeProps = ExtractPropTypes<typeof treeProps>;

export interface ContextMenuItem {
  label: string;
  icon?: string;
  hidden?: boolean;
  disabled?: boolean;
  handler?: Fn;
  divider?: boolean;
  children?: ContextMenuItem[];
}

export interface ContextMenuOptions {
  icon?: string;
  styles?: any;
  items?: ContextMenuItem[];
}

export interface TreeItem extends TreeDataItem {
  icon?: any;
}

export interface TreeActionItem {
  render: (record: Recordable) => any;
  show?: boolean | ((record: Recordable) => boolean);
}

export interface InsertNodeParams {
  parentKey: string | null;
  node: TreeDataItem;
  list?: TreeDataItem[];
  push?: 'push' | 'unshift';
}

export interface TreeActionType {
  checkAll: (checkAll: boolean) => void;
  expandAll: (expandAll: boolean) => void;
  setExpandedKeys: (keys: KeyType[]) => void;
  getExpandedKeys: () => KeyType[];
  setSelectedKeys: (keys: KeyType[]) => void;
  getSelectedKeys: () => KeyType[];
  setCheckedKeys: (keys: CheckKeys) => void;
  getCheckedKeys: () => CheckKeys;
  filterByLevel: (level: number) => void;
  insertNodeByKey: (opt: InsertNodeParams) => void;
  insertNodesByKey: (opt: InsertNodeParams) => void;
  deleteNodeByKey: (key: string) => void;
  updateNodeByKey: (key: string, node: Omit<TreeDataItem, 'key'>) => void;
  setSearchValue: (value: string) => void;
  getSearchValue: () => string;
  getSelectedNode: (
    key: KeyType,
    treeList?: TreeItem[],
    selectNode?: TreeItem | null,
  ) => TreeItem | null;
}
