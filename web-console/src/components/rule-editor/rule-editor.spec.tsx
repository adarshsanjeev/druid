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

import { render } from '@testing-library/react';

import { RuleEditor } from './rule-editor';

describe('RuleEditor', () => {
  it('matches snapshot no tier in rule', () => {
    const ruleEditor = (
      <RuleEditor
        rule={{ type: 'loadForever', tieredReplicants: { test1: 1 } }}
        tiers={['test1', 'test2', 'test3']}
        onChange={() => {}}
        onDelete={() => {}}
        moveUp={undefined}
        moveDown={undefined}
      />
    );
    const { container } = render(ruleEditor);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot with non existing tier in rule', () => {
    const ruleEditor = (
      <RuleEditor
        rule={{
          type: 'loadByInterval',
          interval: '2010-01-01/2015-01-01',
          tieredReplicants: { nonexist: 2 },
        }}
        tiers={['test1', 'test2', 'test3']}
        onChange={() => {}}
        onDelete={() => {}}
        moveUp={undefined}
        moveDown={undefined}
      />
    );
    const { container } = render(ruleEditor);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot with existing tier in rule', () => {
    const ruleEditor = (
      <RuleEditor
        rule={{
          type: 'loadByInterval',
          interval: '2010-01-01/2015-01-01',
          tieredReplicants: { test1: 2 },
        }}
        tiers={['test1', 'test2', 'test3']}
        onChange={() => {}}
        onDelete={() => {}}
        moveUp={undefined}
        moveDown={undefined}
      />
    );
    const { container } = render(ruleEditor);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot with existing tier and non existing tier in rule', () => {
    const ruleEditor = (
      <RuleEditor
        rule={{
          type: 'loadByInterval',
          interval: '2010-01-01/2015-01-01',
          tieredReplicants: {
            test1: 2,
            nonexist: 1,
          },
        }}
        tiers={['test1', 'test2', 'test3']}
        onChange={() => {}}
        onDelete={() => {}}
        moveUp={undefined}
        moveDown={undefined}
      />
    );
    const { container } = render(ruleEditor);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot with broadcast rule', () => {
    const ruleEditor = (
      <RuleEditor
        rule={{
          type: 'broadcastByInterval',
          interval: '2010-01-01/2015-01-01',
        }}
        tiers={[]}
        onChange={() => {}}
        onDelete={() => {}}
        moveUp={undefined}
        moveDown={undefined}
      />
    );
    const { container } = render(ruleEditor);
    expect(container.firstChild).toMatchSnapshot();
  });
});
