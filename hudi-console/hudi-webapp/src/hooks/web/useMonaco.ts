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
import { watch, Ref, unref, ref, computed } from 'vue';
import { until, createEventHook, tryOnUnmounted } from '@vueuse/core';

import type { editor as Editor } from 'monaco-editor';
import setupMonaco from '/@/monaco';
import { isFunction } from '/@/utils/is';
import { ThemeEnum } from '/@/enums/appEnum';
import { useRootSetting } from '../setting/useRootSetting';
const { getDarkMode } = useRootSetting();

export const isDark = computed(() => getDarkMode.value === ThemeEnum.DARK);

export interface MonacoEditorOption {
  code?: any;
  language: string;
  suggestions?: any[];
  options?: Editor.IStandaloneEditorConstructionOptions;
}
export interface TextRange {
  startLineNumber: number;
  endLineNumber: number;
  startColumn: number;
  endColumn: number;
}

export function useMonaco(
  target: Ref,
  options: MonacoEditorOption,
  beforeMount?: (monoao: any) => Promise<void>,
) {
  const changeEventHook = createEventHook<string>();
  const isSetup = ref(false);
  let editor: Editor.IStandaloneCodeEditor;
  let monacoInstance: any;
  const registerCompletionMap = new Map();
  let disposable;
  const setContent = async (content: string) => {
    await until(isSetup).toBeTruthy();
    if (editor) editor.setValue(content);
  };

  const getContent = async () => {
    await until(isSetup).toBeTruthy();
    if (editor) {
      return editor.getValue();
    } else {
      return '';
    }
  };
  const getInstance = async () => {
    await until(isSetup).toBeTruthy();
    if (editor) {
      return editor;
    } else {
      return null;
    }
  };
  const getMonacoInstance = async () => {
    await until(isSetup).toBeTruthy();
    if (monacoInstance) {
      return monacoInstance;
    } else {
      return null;
    }
  };
  let suggestLabels: Array<Recordable> = [];
  const createSuggestions = (
    monaco: any,
    range: TextRange,
    preWord: string,
    currentWord: string,
  ) => {
    const suggestions: Array<Recordable> = [];
    for (let i = 0; i < suggestLabels.length; i++) {
      const id = suggestLabels[i].text;
      const desc = suggestLabels[i].description;
      suggestions.push({
        label: `${id}${desc ? ':' + desc : ''}`,
        insertText: `${preWord !== '{' && currentWord !== '{' ? '{' : ''}${id}}`,
        detail: 'Variable Code',
        kind: monaco.languages.CompletionItemKind.Variable,
        range,
      });
    }
    return suggestions;
  };

  const registerCompletion = async (monaco: any, languageId: string, callbackIds: any[]) => {
    if (callbackIds && callbackIds.length > 0) {
      suggestLabels = callbackIds;
      if (registerCompletionMap.has(languageId)) {
        return;
      }
      registerCompletionMap.set(languageId, 1);
      // remove last suggestions
      disposable?.dispose();

      disposable = monaco.languages.registerCompletionItemProvider(languageId, {
        // triggerCharacters: ['${'],
        triggerCharacters: ['$', '{'],
        provideCompletionItems: async (model, position: Recordable) => {
          const word = model.getWordUntilPosition(position);

          // Gets the full contents of the current input line
          const content = model.getLineContent(position.lineNumber);

          // Pass the subscript to get the content after the current cursor, which is the previous character of the content just entered
          const currentWord = content[position.column - 2];
          const preWord = content[position.column - 3];
          const lastWord = content[position.column - 4];
          if (
            currentWord === '$' ||
            (currentWord === '{' && preWord == '$') ||
            (preWord === '{' && lastWord === '$')
          ) {
            const range: TextRange = {
              startLineNumber: position.lineNumber,
              endLineNumber: position.lineNumber,
              startColumn: word.startColumn,
              endColumn: word.endColumn,
            };
            return {
              suggestions: createSuggestions(monaco, range, preWord, currentWord),
            };
          } else {
            return {
              suggestions: [],
            };
          }
        },
        resolveCompletionItem: () => {
          return [{ label: 'sss' }];
        },
      });
    }
  };

  const setMonacoSuggest = async (suggestions: any[]) => {
    await until(isSetup).toBeTruthy();
    if (monacoInstance) {
      registerCompletion(monacoInstance, options.language, suggestions);
    }
  };

  const disposeInstance = async () => {
    editor?.dispose();
  };

  const init = async () => {
    const { monaco } = await setupMonaco();
    monacoInstance = monaco;
    if (isFunction(beforeMount)) await beforeMount(monaco);

    watch(
      target,
      () => {
        const el = unref(target);
        if (!el) {
          return;
        }
        const model = monaco.editor.createModel(options.code, options.language);
        const defaultOptions = {
          model,
          language: options.language,
          tabSize: 2,
          insertSpaces: true,
          autoClosingQuotes: 'always',
          detectIndentation: false,
          folding: false,
          automaticLayout: true,
          theme: 'vs',
          minimap: {
            enabled: false,
          },
        };
        editor = monaco.editor.create(el, Object.assign(defaultOptions, options.options || {}));
        isSetup.value = true;
        if (!options.options?.theme) {
          watch(
            isDark,
            () => {
              if (isDark.value) monaco.editor.setTheme('vs-dark');
              else monaco.editor.setTheme('vs');
            },
            { immediate: true },
          );
        }

        editor.getModel()?.onDidChangeContent(() => {
          changeEventHook.trigger(editor.getValue());
        });
      },
      {
        flush: 'post',
        immediate: true,
      },
    );
  };

  init();

  tryOnUnmounted(() => {
    stop();
    disposable?.dispose();
    disposeInstance();
  });

  return {
    onChange: changeEventHook.on,
    setContent,
    setMonacoSuggest,
    getContent,
    getInstance,
    getMonacoInstance,
    disposeInstance,
  };
}
