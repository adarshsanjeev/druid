/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import escape from 'lodash.escape';

import { assemble } from '../utils';

export interface ItemDescription {
  name: string;
  syntax?: string;
  description?: string;
}

export function makeDocHtml(item: ItemDescription) {
  return assemble(
    `<div class="doc-name">${item.name}</div>`,
    item.syntax ? `<div class="doc-syntax">${escape(item.syntax)}</div>` : undefined,
    item.description ? `<div class="doc-description">${item.description}</div>` : undefined,
  ).join('\n');
}
