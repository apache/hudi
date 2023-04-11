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
import { getCurrentInstance, onMounted } from 'vue';
import * as monaco from 'monaco-editor';
import editorWorker from 'monaco-editor/esm/vs/editor/editor.worker?worker';
import jsonWorker from 'monaco-editor/esm/vs/language/json/json.worker?worker';
import cssWorker from 'monaco-editor/esm/vs/language/css/css.worker?worker';
import htmlWorker from 'monaco-editor/esm/vs/language/html/html.worker?worker';
import tsWorker from 'monaco-editor/esm/vs/language/typescript/ts.worker?worker';

monaco.languages.registerCompletionItemProvider('xml', {
  // @ts-ignore
  provideCompletionItems: function (model, position) {
    const textUntilPosition = model.getValueInRange({
      startLineNumber: 1,
      startColumn: 1,
      endLineNumber: position.lineNumber,
      endColumn: position.column,
    });
    //dependency...
    if (textUntilPosition.match(/\s*<dep(.*)\s*\n*(.*\n*)*(<\/dependency>|)?$/)) {
      const word = model.getWordUntilPosition(position);
      const range = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      };
      const suggestions = [
        {
          label: '"dependency"',
          insertText:
            'dependency>\n' +
            '    <groupId></groupId>\n' +
            '    <artifactId></artifactId>\n' +
            '    <version></version>\n' +
            '</dependency',
        },
        { label: '"group"', insertText: 'groupId></groupId' },
        { label: '"artifactId"', insertText: 'artifactId></artifactId' },
        { label: '"version"', insertText: 'version></version' },
      ];

      if (textUntilPosition.indexOf('<exclusions>') > 0) {
        suggestions.push({
          label: '"exclusion"',
          insertText:
            'exclusion>\n' +
            '  <artifactId></artifactId>\n' +
            '  <groupId></groupId>\n' +
            '</exclusion',
        });
      } else {
        suggestions.push({
          label: '"exclusions"',
          insertText:
            'exclusions>\n' +
            '  <exclusion>\n' +
            '    <artifactId></artifactId>\n' +
            '    <groupId></groupId>\n' +
            '  </exclusion>\n' +
            '</exclusions',
        });
      }
      suggestions.forEach((x: any) => {
        x.kind = monaco.languages.CompletionItemKind.Function;
        x.range = range;
      });
      return { suggestions: suggestions };
    }
  },
});

const setup = async () => {
  monaco.languages.typescript.javascriptDefaults.setCompilerOptions({
    ...monaco.languages.typescript.javascriptDefaults.getCompilerOptions(),
    noUnusedLocals: false,
    noUnusedParameters: false,
    allowUnreachableCode: true,
    allowUnusedLabels: true,
    strict: false,
    allowJs: true,
  });

  window.MonacoEnvironment = {
    getWorker(_, label) {
      if (label === 'json') {
        return new jsonWorker();
      }
      if (label === 'css' || label === 'scss' || label === 'less') {
        return new cssWorker();
      }
      if (label === 'html' || label === 'handlebars' || label === 'razor') {
        return new htmlWorker();
      }
      if (label === 'typescript' || label === 'javascript') {
        return new tsWorker();
      }
      return new editorWorker();
    },
  };

  if (getCurrentInstance()) await new Promise<void>((resolve) => onMounted(resolve));

  return { monaco };
};

export default setup;

setup();
