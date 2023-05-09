package org.apache.pulsar.io.gcp;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

@Slf4j
public class PubsubSink implements Sink<GenericObject> {

  private PubsubConfig pubsubConfig = null;
  private Publisher publisher = null;

  @Override
  public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
    pubsubConfig = PubsubConfig.load(config);
    Objects.requireNonNull(pubsubConfig.getProjectId(), "Project is not set");
    TopicName topicName = TopicName.of(pubsubConfig.getProjectId(), pubsubConfig.getTopicId());
    this.publisher = Publisher.newBuilder(topicName).build();
  }

  @Override
  public void write(Record<GenericObject> record) throws Exception {
    // this.publisher.publish(
    //     PubsubMessage.parseFrom(ByteString.copyFrom(record.getMessage().get().getData())));

    ByteString data = ByteString.copyFromUtf8("abc");
    PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
    this.publisher.publish(pubsubMessage);
  }

  @Override
  public void close() {
    if (this.publisher != null) {
      this.publisher.shutdown();
    }
  }
}
