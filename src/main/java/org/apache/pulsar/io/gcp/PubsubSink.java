// Copyright 2023 Tianzi Cai
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.pulsar.io.gcp;

import com.google.api.gax.batching.BatchingSettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.gcp.PubsubSinkConfig;

@Slf4j
public class PubsubSink implements Sink<GenericObject> {

  private Publisher publisher = null;

  @Override
  public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
    PubsubSinkConfig pubsubSinkConfig = PubsubSinkConfig.load(config);
    TopicName topicName =
        TopicName.of(pubsubSinkConfig.getProjectId(), pubsubSinkConfig.getTopicId());
    BatchingSettings batchingSettings =
        BatchingSettings.newBuilder()
            .setElementCountThreshold(pubsubSinkConfig.getBatchSize())
            .build();
    this.publisher = Publisher.newBuilder(topicName).setBatchingSettings(batchingSettings).build();
  }

  @Override
  public void write(Record<GenericObject> record) {
    if (record.getMessage().isPresent()) {
      ByteString data = ByteString.copyFrom(record.getMessage().get().getData());
      PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
      this.publisher.publish(pubsubMessage);
    }
  }

  @Override
  public void close() {
    if (this.publisher != null) {
      this.publisher.shutdown();
      log.info("Pub/Sub sink has been shut down.");
    }
  }
}
