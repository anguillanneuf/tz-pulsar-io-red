package org.apache.pulsar.io.gcp;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

@Slf4j
public class PubsubSource implements Source<byte[]> {
  private static final int DEFAULT_QUEUE_LENGTH = 100;
  private Subscriber subscriber = null;
  private LinkedBlockingQueue<Record<byte[]>> queue;

  @Override
  public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
    queue = new LinkedBlockingQueue<>(DEFAULT_QUEUE_LENGTH);

    PubsubSourceConfig pubsubSourceConfig = PubsubSourceConfig.load(config);

    ProjectSubscriptionName projectSubscriptionName =
        ProjectSubscriptionName.of(pubsubSourceConfig.getProjectId(), pubsubSourceConfig.getSubscriptionId());

    Subscriber.Builder subscriberBuilder = Subscriber.newBuilder(projectSubscriptionName,
        (PubsubMessage message, AckReplyConsumer consumer) -> {
          Record<byte[]> record = new PubsubRecord(sourceContext.getOutputTopic(), message);
          try {
            queue.put(record);
            consumer.ack();
          } catch (InterruptedException e) {
            consumer.nack();
            throw new RuntimeException(e);
          }
        });
    subscriber = subscriberBuilder.build();
    subscriber.startAsync().awaitRunning();
    log.info("listening for messages on {}..", subscriber.getSubscriptionNameString());

  }

  @Override
  public Record<byte[]> read() throws Exception {
    return this.queue.take();
  }

  @Override
  public void close() {
    if (this.subscriber != null) {
      this.subscriber.stopAsync().awaitRunning();
    }
  }

  private record PubsubRecord(String pulsarTopic, PubsubMessage pubsubMessage) implements
      Record<byte[]> {
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
    }
}
