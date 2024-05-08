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

package org.apache.druid.storage.s3.output;


import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.StorageConnectorProvider;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;

import java.io.File;

import static org.apache.druid.server.metrics.DataSourceTaskIdHolder.TMP_DIR_BINDING;

@JsonTypeName(S3StorageDruidModule.SCHEME)
public class S3StorageConnectorProvider extends S3OutputConfig implements StorageConnectorProvider
{
  @JacksonInject
  ServerSideEncryptingAmazonS3 s3;
  @JacksonInject
  Injector injector;

  @JsonCreator
  public S3StorageConnectorProvider(
      @JsonProperty(value = "bucket", required = true) String bucket,
      @JsonProperty(value = "prefix", required = true) String prefix,
      @JsonProperty("chunkSize") HumanReadableBytes chunkSize,
      @JsonProperty("maxRetry") Integer maxRetry
  )
  {
    super(bucket, prefix, chunkSize, maxRetry);
  }

  @Override
  public StorageConnector get()
  {
    final File tempDir = injector.getInstance(Key.get(File.class, Names.named(TMP_DIR_BINDING)));
    return new S3StorageConnector(this, s3, tempDir);
  }
}
