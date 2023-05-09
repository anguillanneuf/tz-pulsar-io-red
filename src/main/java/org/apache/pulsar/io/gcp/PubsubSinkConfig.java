package org.apache.pulsar.io.gcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Data
@Accessors(chain = true)
public class PubsubSinkConfig implements Serializable {

  @FieldDoc(required = true, defaultValue = "", help = "Google Cloud project ID")
  private String projectId = "";

  @FieldDoc(required = true, defaultValue = "", help = "Google Cloud Pub/Sub topic ID")
  private String topicId = "";

  @FieldDoc(required = true, defaultValue = "", help = "Publisher batch size")
  private Long batchSize = 10L;

  public static PubsubSinkConfig load(Map<String, Object> map) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(new ObjectMapper().writeValueAsString(map), PubsubSinkConfig.class);
  }
}
