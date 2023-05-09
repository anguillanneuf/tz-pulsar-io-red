package org.apache.pulsar.io.gcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Data
@Accessors(chain = true)
public class PubsubConfig implements Serializable {

  @FieldDoc(required = true, defaultValue = "", help = "Google Cloud project ID")
  private String projectId = "";

  @FieldDoc(required = true, defaultValue = "", help = "Google Cloud Pub/Sub topic ID")
  private String topicId = "";

  public void validate() {
    if (projectId == null || projectId.equals("")) {
      throw new IllegalArgumentException("projectId is required");
    }

    if (topicId == null || topicId.equals("")) {
      throw new IllegalArgumentException("topicId is required");
    }
  }

  public static PubsubConfig load(String yamlFile) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    return mapper.readValue(new File(yamlFile), PubsubConfig.class);
  }

  public static PubsubConfig load(Map<String, Object> map) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(new ObjectMapper().writeValueAsString(map), PubsubConfig.class);
  }
}
