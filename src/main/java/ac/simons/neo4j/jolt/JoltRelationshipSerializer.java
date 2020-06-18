/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ac.simons.neo4j.jolt;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.neo4j.graphdb.Relationship;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

final class JoltRelationshipSerializer extends StdSerializer<Relationship> {

	JoltRelationshipSerializer() {
		super(Relationship.class);
	}

	@Override
	public void serialize(Relationship relationship, JsonGenerator generator, SerializerProvider provider)
		throws IOException {

		generator.writeStartObject(relationship);
		generator.writeFieldName(Sigil.RELATIONSHIP.getValue());

		generator.writeStartObject();

		generator.writeFieldName("id");
		generator.writeObject(relationship.getId());

		generator.writeFieldName("type");
		generator.writeObject(relationship.getType());

		generator.writeFieldName("startNodeId");
		generator.writeObject(relationship.getStartNodeId());

		generator.writeFieldName("endNodeId");
		generator.writeObject(relationship.getEndNodeId());

		generator.writeFieldName("properties");
		var properties = Optional.ofNullable(relationship.getAllProperties()).orElseGet(Map::of);
		generator.writeObject(properties);

		generator.writeEndObject();
		generator.writeEndObject();
	}
}
