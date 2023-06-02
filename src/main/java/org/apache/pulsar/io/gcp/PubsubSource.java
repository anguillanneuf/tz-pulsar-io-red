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

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;

@Slf4j
public class PubsubSource extends PushSource<byte[]> {
  private Subscriber subscriber = null;

  @Override
  public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {

    PubsubSourceConfig pubsubSourceConfig = PubsubSourceConfig.load(config);

    ProjectSubscriptionName projectSubscriptionName =
        ProjectSubscriptionName.of(pubsubSourceConfig.getProjectId(), pubsubSourceConfig.getSubscriptionId());

    ExecutorProvider executorProvider =
        InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(4).build();

    FlowControlSettings flowControlSettings =
        FlowControlSettings.newBuilder()
            .setMaxOutstandingElementCount(pubsubSourceConfig.getFlowSize())
            .build();

    Subscriber.Builder subscriberBuilder = Subscriber.newBuilder(projectSubscriptionName,
        (PubsubMessage message, AckReplyConsumer consumer) -> {
          Record<byte[]> record = new PubsubRecord(sourceContext.getOutputTopic(), message,
              consumer);
          consume(record);
        });
    subscriberBuilder.setFlowControlSettings(flowControlSettings);
    subscriberBuilder.setParallelPullCount(pubsubSourceConfig.getNumStreams());
    subscriberBuilder.setExecutorProvider(executorProvider);
    subscriber = subscriberBuilder.build();
    subscriber.startAsync().awaitRunning();
    log.info("listening for messages on {}..", subscriber.getSubscriptionNameString());
  }

  @Override
  public void close() {
    if (this.subscriber != null) {
      this.subscriber.stopAsync().awaitRunning();
    }
  }

  private record PubsubRecord(String pulsarTopic, PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) implements Record<byte[]> {
    @Override
    public Optional<String> getKey() {
      if (!this.pubsubMessage.getOrderingKey().isEmpty()) {
        return Optional.of(this.pubsubMessage.getOrderingKey());
      } else {
        return Optional.empty();
      }
    }

    @Override
    public byte[] getValue() {
      return this.pubsubMessage.getData().toByteArray();
    }

    @Override
    public Optional<Long> getEventTime() {
      return Optional.of(this.pubsubMessage.getPublishTime().getSeconds());
    }

    @Override
    public Map<String, String> getProperties() {
      return this.pubsubMessage.getAttributesMap();
    }

    @Override
    public Optional<String> getDestinationTopic() {
      return Optional.of(this.pulsarTopic);
    }

    @Override
    public void ack(){ ackReplyConsumer.ack(); }

    @Override
    public void fail() { ackReplyConsumer.nack(); }
  }
}
