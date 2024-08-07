/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.error;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class ExceptionTest
{
  @Test
  public void testNoCause()
  {
    DruidException exception = DruidException.defensive().build("defensive");
    StackTraceElement[] stackTrace = exception.getStackTrace();
    for (StackTraceElement stackTraceElement : stackTrace) {
      Assert.assertFalse(stackTraceElement.getClassName().startsWith(DruidException.CLASS_NAME_STR));
    }
  }

  @Test
  public void testNoStacktrace()
  {
    ErrorResponse errorResponse = new ErrorResponse(Forbidden.exception());
    final Map<String, Object> asMap = errorResponse.getAsMap();
    DruidException exception = ErrorResponse.fromMap(asMap).getUnderlyingException();
    Assert.assertTrue(exception.getCause() instanceof DruidException);
    Assert.assertEquals(0, exception.getCause().getStackTrace().length);
  }
}
