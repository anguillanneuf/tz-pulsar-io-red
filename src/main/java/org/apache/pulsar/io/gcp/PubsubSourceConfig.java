package org.apache.pulsar.io.gcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Data
@Accessors(chain = true)
public class PubsubSourceConfig {

  @FieldDoc(required = true, defaultValue = "", help = "Google Cloud project ID")
  private String projectId = "";

  @FieldDoc(required = true, defaultValue = "", help = "Google Cloud Pub/Sub subscription ID")
  private String subscriptionId = "";

  @FieldDoc(required = true, defaultValue = "", help = "Subscriber flow size")
  private Long flowSize = 10L;

  public static PubsubSourceConfig load(Map<String, Object> map) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(new ObjectMapper().writeValueAsString(map), PubsubSourceConfig.class);
  }
}
