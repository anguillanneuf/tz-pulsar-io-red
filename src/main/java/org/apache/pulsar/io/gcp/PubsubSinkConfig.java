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
