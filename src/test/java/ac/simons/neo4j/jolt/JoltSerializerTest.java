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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.Values;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JoltSerializerTest {

	private final ObjectMapper objectMapper;

	JoltSerializerTest() {

		this.objectMapper = new ObjectMapper();
		this.objectMapper.registerModule(JoltModule.STRICT.getInstance());
	}

	@Nested
	class SpareMode {

		private ObjectMapper spareObjectMapper;

		SpareMode() {

			this.spareObjectMapper = new ObjectMapper();
			this.spareObjectMapper.registerModule(JoltModule.DEFAULT.getInstance());
		}

		@Test
		void shouldUseJSONString() throws JsonProcessingException {

			spareObjectMapper = new ObjectMapper();
			var result = spareObjectMapper.writeValueAsString("Hello, World");
			assertThat(result).isEqualTo("\"Hello, World\"");
		}

		@Test
		void shouldUseJSONBoolean() throws JsonProcessingException {

			spareObjectMapper = new ObjectMapper();
			var result = spareObjectMapper.writeValueAsString(true);
			assertThat(result).isEqualTo("true");
		}

		@Test
		void shouldUseJSONList() throws JsonProcessingException {

			var result = spareObjectMapper.writeValueAsString(List.of(1, 2, "3"));
			assertThat(result).isEqualTo("[1,2,\"3\"]");
		}
	}

	@Nested
	class SimpleTypes {

		@Test
		void shouldSerializeInteger() throws JsonProcessingException {

			var result = objectMapper.writeValueAsString(123);
			assertThat(result).isEqualTo("{\"Z\":\"123\"}");
		}

		@Test
		void shouldSerializeBoolean() throws JsonProcessingException {

			var result = objectMapper.writeValueAsString(true);
			assertThat(result).isEqualTo("{\"?\":\"true\"}");
		}

		@Test
		void shouldSerializeLong() throws JsonProcessingException {

			var result = objectMapper.writeValueAsString(123L);
			assertThat(result).isEqualTo("{\"Z\":\"123\"}");
		}

		@Test
		void shouldSerializeDouble() throws JsonProcessingException {

			var result = objectMapper.writeValueAsString(42.23);
			assertThat(result).isEqualTo("{\"R\":\"42.23\"}");
		}

		@Test
		void shouldSerializeString() throws JsonProcessingException {

			var result = objectMapper.writeValueAsString("Hello, World");
			assertThat(result).isEqualTo("{\"U\":\"Hello, World\"}");
		}

		@Test
		void shouldSerializePoint() throws JsonProcessingException {

			var point = Values.pointValue(CoordinateReferenceSystem.WGS84, 12.994823, 55.612191);
			var result = objectMapper.writeValueAsString(point);
			assertThat(result).isEqualTo("{\"@\":\"SRID=4326;POINT(12.994823 55.612191)\"}");
		}
	}

	@Nested
	@TestInstance(TestInstance.Lifecycle.PER_CLASS)
	class TemporalTypes {

		Stream<Arguments> temporals() {
			return Stream.of(
				Arguments.of(LocalDate.of(2020, 12, 14), "2020-12-14"),
				Arguments.of(OffsetTime.of(LocalTime.of(21, 21, 0), ZoneOffset.ofHours(4)), "21:21:00+04:00"),
				Arguments.of(LocalTime.of(21, 21, 0), "21:21:00"),
				Arguments.of(ZonedDateTime.of(
					LocalDate.of(2020, 12, 14), LocalTime.of(17, 14, 0), ZoneId.of("Europe/Berlin")),
					"2020-12-14T17:14:00+01:00[Europe/Berlin]"),
				Arguments.of(LocalDateTime.of(LocalDate.of(2020, 12, 14), LocalTime.of(17, 14, 0)),
					"2020-12-14T17:14:00")
			);
		}

		@ParameterizedTest
		@MethodSource("temporals")
		void shouldSerializeTemporals(Temporal t, String expectedValue) throws JsonProcessingException {

			var result = objectMapper.writeValueAsString(t);
			assertThat(result).isEqualTo(String.format("{\"T\":\"%s\"}", expectedValue));
		}

		@Test
		void shouldSerializeDurationValue() throws JsonProcessingException {

			var result = objectMapper.writeValueAsString(DurationValue.duration(Duration.ofHours(23).plusMinutes(21)));
			assertThat(result).isEqualTo(String.format("{\"T\":\"%s\"}", "PT23H21M"));

			result = objectMapper.writeValueAsString(DurationValue.duration(Period.ofDays(42)));
			assertThat(result).isEqualTo(String.format("{\"T\":\"%s\"}", "P42D"));
		}
	}

	@Nested
	class Arrays {

		@Test
		void shouldSerializeLongArray() throws JsonProcessingException {

			var result = objectMapper.writeValueAsString(new Long[] { 0L, 1L, 2L });
			assertThat(result).isEqualTo("[{\"Z\":\"0\"},{\"Z\":\"1\"},{\"Z\":\"2\"}]");
		}

		@Test
		void shouldSerializeByteArray() throws JsonProcessingException {

			var result = objectMapper
				.writeValueAsString(new byte[] { 0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16 });
			assertThat(result).isEqualTo("{\"#\":\"0001020304050608090A0B0C0D0E0F10\"}");
		}
	}

	@Nested
	class Collections {

		@Test
		void shouldSerializeArrays() throws JsonProcessingException {

			var result = objectMapper.writeValueAsString(new String[] { "A", "B" });
			assertThat(result).isEqualTo("[{\"U\":\"A\"},{\"U\":\"B\"}]");
		}

		@Test
		void shouldSerializeHomogenousList() throws JsonProcessingException {

			var result = objectMapper.writeValueAsString(List.of(1, 2, 3));
			assertThat(result).isEqualTo("{\"[]\":[{\"Z\":\"1\"},{\"Z\":\"2\"},{\"Z\":\"3\"}]}");
		}

		@Test
		void shouldSerializeHeterogeneousList() throws JsonProcessingException {

			var result = objectMapper.writeValueAsString(List.of("A", 21, 42.3));
			assertThat(result).isEqualTo("{\"[]\":[{\"U\":\"A\"},{\"Z\":\"21\"},{\"R\":\"42.3\"}]}");
		}

		@Test
		void shouldSerializeMap() throws JsonProcessingException {

			// Treemap only created to have a stable iterator for a non flaky test ;)
			var result = objectMapper.writeValueAsString(new TreeMap<>(Map.of("name", "Alice", "age", 33)));
			assertThat(result).isEqualTo("{\"{}\":{\"age\":{\"Z\":\"33\"},\"name\":{\"U\":\"Alice\"}}}");
		}
	}

	@Nested
	@ExtendWith(MockitoExtension.class)
	class Entities {

		@Test
		void prettyPrintedNode(@Mock Node node) throws JsonProcessingException {

			when(node.getId()).thenReturn(123L);
			when(node.getLabels())
				.thenReturn(List.of(Label.label("Person"), Label.label("Female"), Label.label("Human Being")));
			when(node.getAllProperties()).thenReturn(new TreeMap<>(Map.of("name", "Alice", "age", 33)));
			var writer = objectMapper.writerWithDefaultPrettyPrinter();
			var result = writer.writeValueAsString(node);
			assertThat(result).isEqualTo("{\n"
										 + "  \"()\" : [ 123, [ \"Person\", \"Female\", \"Human Being\" ], {\n"
										 + "    \"age\" : {\n"
										 + "      \"Z\" : \"33\"\n"
										 + "    },\n"
										 + "    \"name\" : {\n"
										 + "      \"U\" : \"Alice\"\n"
										 + "    }\n"
										 + "  } ]\n"
										 + "}");
		}

		@Test
		void shouldSerializeNodes(@Mock Node node) throws JsonProcessingException {

			when(node.getId()).thenReturn(4711L);
			when(node.getLabels()).thenReturn(List.of(Label.label("A"), Label.label("B")));
			when(node.getAllProperties()).thenReturn(new TreeMap<>(Map.of("prop1", 1, "prop2", "Peng")));
			var result = objectMapper.writeValueAsString(node);
			assertThat(result).isEqualTo(
				"{\"()\":[4711,[\"A\",\"B\"],{\"prop1\":{\"Z\":\"1\"},\"prop2\":{\"U\":\"Peng\"}}]}");
		}

		@Test
		void shouldSerializeNodesWithoutLabels(@Mock Node node) throws JsonProcessingException {

			when(node.getId()).thenReturn(4711L);
			when(node.getLabels()).thenReturn(List.of());
			var result = objectMapper.writeValueAsString(node);
			assertThat(result)
				.isEqualTo("{\"()\":[4711,[],{}]}");
		}

		@Test
		void shouldSerializeRelationships(@Mock Relationship relationship) throws JsonProcessingException {

			when(relationship.getId()).thenReturn(4711L);
			when(relationship.getType()).thenReturn(RelationshipType.withName("KNOWS"));
			when(relationship.getStartNodeId()).thenReturn(123L);
			when(relationship.getEndNodeId()).thenReturn(124L);
			when(relationship.getAllProperties()).thenReturn(Map.of("since", 1999));

			var result = objectMapper.writeValueAsString(relationship);
			assertThat(result)
				.isEqualTo(
					"{\"->\":[4711,123,\"KNOWS\",124,{\"since\":{\"Z\":\"1999\"}}]}");

		}
	}
}
