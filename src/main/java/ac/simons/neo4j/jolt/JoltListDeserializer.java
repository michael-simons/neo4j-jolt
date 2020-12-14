/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package ac.simons.neo4j.jolt;

import java.io.IOException;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.values.storable.PointValue;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.type.CollectionLikeType;
import com.fasterxml.jackson.databind.type.TypeFactory;

final class JoltListDeserializer extends JoltStdDeserializer<List<?>> {
	static final CollectionLikeType HANDLED_TYPE = TypeFactory
		.defaultInstance().constructCollectionLikeType(List.class, Object.class);

	JoltListDeserializer() {
		super(HANDLED_TYPE, Sigil.LIST);
	}

	public void serialize(List<?> list, JsonGenerator generator, SerializerProvider provider) throws IOException {
		generator.writeStartObject(list);
		generator.writeFieldName(Sigil.LIST.getValue());
		generator.writeStartArray(list);

		for (var entry : list) {
			generator.writeObject(entry);
		}

		generator.writeEndArray();
		generator.writeEndObject();
	}

	@Override
	protected boolean restrictToStringValues() {
		return false;
	}

	@Override
	List<?> deserializeImpl(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {

		if (!p.isExpectedStartArrayToken()) {
			throw new InvalidFormatException(p, "Jolt list type must be expressed as an array.", null, List.class);
		}

		JsonToken t;
		List result = new ArrayList();
		while ((t = p.nextToken()) != JsonToken.END_ARRAY) {

			// Yikes, as we support heterogeneous lists, we need to parse each value for the sigil :(
			if (t.isStructStart()) {
                result.add(handleNestedJolt(p, ctxt));
			} else {
				throw new RuntimeException("Not yet done.");
			}
		}

		return result;
	}

	Object handleNestedJolt(JsonParser p, DeserializationContext ctxt) throws IOException {
        System.out.println(p.currentName());
        System.out.println(p.getCurrentToken());
        System.out.println("---");
		Sigil sigil = getSigilFromStruct(p);
		switch (sigil) {
			case TIME:
				return ctxt.readValue(p, Temporal.class);
            case INTEGER:
            case REAL:
                return ctxt.readValue(p, Number.class);
            case BOOLEAN:
                return ctxt.readValue(p, Boolean.class);
            case LIST:
                return ctxt.readValue(p, List.class);
            case SPATIAL:
                return ctxt.readValue(p, PointValue.class);
            case BINARY:
                return ctxt.readValue(p, byte[].class);
            case UNICODE:
                return ctxt.readValue(p, String.class);
            default:
                throw new InvalidFormatException(p, String.format("Unsupported Jolt type '%s':", sigil), null, List.class);
		}
	}
}
