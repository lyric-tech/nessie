/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.events.custom.kafka.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.inject.spi.CDI;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.projectnessie.events.api.Event;
import org.projectnessie.model.ser.Views;

/** Kafka serializer that converts Nessie {@link Event} objects to JSON bytes using Jackson. */
public class KafkaJsonEventSerializer implements Serializer<Event> {

  private final ObjectMapper objectMapper;

  public KafkaJsonEventSerializer() {
    objectMapper = CDI.current().select(ObjectMapper.class).get();
  }

  @Override
  public byte[] serialize(String topic, Event data) {
    if (data == null) {
      return null;
    }
    try {
      return objectMapper.writerWithView(Views.V2.class).writeValueAsBytes(data);
    } catch (Exception e) {
      throw new SerializationException("Error serializing Nessie event to JSON", e);
    }
  }
}
