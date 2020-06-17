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
import java.util.Collections;
import java.util.Optional;

import org.neo4j.graphdb.Node;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

final class JoltNodeSerializer extends StdSerializer<Node> {

	JoltNodeSerializer() {
		super(Node.class);
	}

	@Override
	public void serialize(Node node, JsonGenerator generator, SerializerProvider provider) throws IOException {

		generator.writeStartObject(node);
		generator.writeFieldName(Sigil.NODE.getValue());

		generator.writeStartObject();

		generator.writeFieldName("id");
		generator.writeObject(node.getId());

		generator.writeFieldName("labels");
		generator.writeObject(node.getLabels());

		generator.writeFieldName("properties");
		var properties = Optional.ofNullable(node.getAllProperties()).orElseGet(Collections::emptyMap);
		generator.writeObject(properties);

		generator.writeEndObject();
		generator.writeEndObject();
	}
}
