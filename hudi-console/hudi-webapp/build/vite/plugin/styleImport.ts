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
/**
 *  Introduces component library styles on demand.
 * https://github.com/anncwb/vite-plugin-style-import
 */
import { createStyleImportPlugin } from 'vite-plugin-style-import';

export function configStyleImportPlugin(_isBuild: boolean) {
  // if (!isBuild) {
  //   return [];
  // }
  const styleImportPlugin = createStyleImportPlugin({
    libs: [
      {
        libraryName: 'ant-design-vue',
        esModule: true,
        resolveStyle: (name) => {
          // Here is a list of "subcomponents" that do not require additional style files
          const ignoreList = [
            'anchor-link',
            'sub-menu',
            'menu-item',
            'menu-divider',
            'menu-item-group',
            'breadcrumb-item',
            'breadcrumb-separator',
            'form-item',
            'step',
            'select-option',
            'select-opt-group',
            'card-grid',
            'card-meta',
            'collapse-panel',
            'descriptions-item',
            'list-item',
            'list-item-meta',
            'table-column',
            'table-column-group',
            'tab-pane',
            'tab-content',
            'timeline-item',
            'tree-node',
            'skeleton-input',
            'skeleton-avatar',
            'skeleton-title',
            'skeleton-paragraph',
            'skeleton-image',
            'skeleton-button',
          ];
          // Here is a list of subcomponents that require additional styles
          // When importing sub-components separately, you need to import component styles, otherwise the styles of sub-components will be lost after packaging
          const replaceList = {
            'typography-text': 'typography',
            'typography-title': 'typography',
            'typography-paragraph': 'typography',
            'typography-link': 'typography',
            'dropdown-button': 'dropdown',
            'input-password': 'input',
            'input-search': 'input',
            'input-group': 'input',
            'radio-group': 'radio',
            'checkbox-group': 'checkbox',
            'layout-sider': 'layout',
            'layout-content': 'layout',
            'layout-footer': 'layout',
            'layout-header': 'layout',
            'month-picker': 'date-picker',
            'range-picker': 'date-picker',
          };

          return ignoreList.includes(name)
            ? ''
            : replaceList.hasOwnProperty(name)
            ? `ant-design-vue/es/${replaceList[name]}/style/index`
            : `ant-design-vue/es/${name}/style/index`;
        },
      },
    ],
  });
  return styleImportPlugin;
}
