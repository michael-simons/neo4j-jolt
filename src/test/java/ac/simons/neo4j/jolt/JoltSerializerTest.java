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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.sql.Time;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.Values;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JoltSerializerTest {

	private final ObjectMapper objectMapper;

	JoltSerializerTest() {

		this.objectMapper = new ObjectMapper();
		this.objectMapper.registerModule(JoltModule.DEFAULT.getInstance());
	}

	@Nested
	class SparseMode
	{

		private ObjectMapper spareObjectMapper;

		SparseMode() {

			this.spareObjectMapper = new ObjectMapper();
			this.spareObjectMapper.registerModule(JoltModule.SPARSE.getInstance());
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
		void shouldSerializeNull() throws JsonProcessingException
		{
			var result = objectMapper.writeValueAsString( null );
			assertThat( result ).isEqualTo( "null" );
		}

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
		void shouldSerializeLongInsideInt32Range() throws JsonProcessingException {

			var result = objectMapper.writeValueAsString(123L);
			assertThat(result).isEqualTo("{\"Z\":\"123\"}");
		}

		@Test
		void shouldSerializeLongAboveInt32Range() throws JsonProcessingException {

			var result = objectMapper.writeValueAsString((long) Integer.MAX_VALUE + 1);
			assertThat(result).isEqualTo("{\"R\":\"2147483648\"}");
		}

		@Test
		void shouldSerializeLongBelowInt32Range() throws JsonProcessingException {

			var result = objectMapper.writeValueAsString((long) Integer.MIN_VALUE - 1);
			assertThat(result).isEqualTo("{\"R\":\"-2147483649\"}");
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
	class DateTimeDuration
	{
		@Test
		void shouldSerializeDuration() throws JsonProcessingException {
			var duration = DurationValue.duration( Duration.ofDays( 20 ) );
			var result = objectMapper.writeValueAsString(duration);
			assertThat(result).isEqualTo("{\"T\":\"PT480H\"}");
		}

		@Test
		void shouldSerializeLargeDuration() throws JsonProcessingException {
			var durationString = "P3Y6M4DT12H30M5S";
			var durationValue = DurationValue.parse( durationString );
			var result = objectMapper.writeValueAsString(durationValue);
			assertThat(result).isEqualTo("{\"T\":\"" + durationString + "\"}");
		}

		@Test
		void shouldSerializeDate() throws JsonProcessingException {
			var dateString = "2020-08-25";
			var date = LocalDate.parse( dateString );
			var result = objectMapper.writeValueAsString(date);
			assertThat(result).isEqualTo("{\"T\":\"" +dateString +"\"}");
		}

		@Test
		void shouldSerializeTime() throws JsonProcessingException {
			var timeString = "12:52:58.513775";
			var time = LocalTime.parse(timeString);
			var result = objectMapper.writeValueAsString(time);
			assertThat(result).isEqualTo("{\"T\":\"" +timeString +"\"}");
		}

		@Test
		void shouldSerializeOffsetTime() throws JsonProcessingException {
			var offsetTimeString = "12:55:10.775607+01:00";
			var time = OffsetTime.parse(offsetTimeString);
			var result = objectMapper.writeValueAsString(time);
			assertThat(result).isEqualTo("{\"T\":\"" +offsetTimeString +"\"}");
		}

		@Test
		void shouldSerializeLocalDateTime() throws JsonProcessingException {
			var localDateTimeString = "2020-08-25T12:57:36.069665";
			var dateTime = LocalDateTime.parse(localDateTimeString);
			var result = objectMapper.writeValueAsString(dateTime);
			assertThat(result).isEqualTo("{\"T\":\"" +localDateTimeString +"\"}");
		}

		@Test
		void shouldSerializeZonedDateTime() throws JsonProcessingException {
			var zonedDateTimeString = "2020-08-25T13:03:39.11733+01:00[Europe/London]";
			var dateTime = ZonedDateTime.parse(zonedDateTimeString);
			var result = objectMapper.writeValueAsString(dateTime);
			assertThat(result).isEqualTo("{\"T\":\"" +zonedDateTimeString +"\"}");
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
				+ "  \"()\" : {\n"
				+ "    \"id\" : {\n"
				+ "      \"Z\" : \"123\"\n"
				+ "    },\n"
				+ "    \"labels\" : {\n"
				+ "      \"[]\" : [ {\n"
				+ "        \"U\" : \"Person\"\n"
				+ "      }, {\n"
				+ "        \"U\" : \"Female\"\n"
				+ "      }, {\n"
				+ "        \"U\" : \"Human Being\"\n"
				+ "      } ]\n"
				+ "    },\n"
				+ "    \"properties\" : {\n"
				+ "      \"{}\" : {\n"
				+ "        \"age\" : {\n"
				+ "          \"Z\" : \"33\"\n"
				+ "        },\n"
				+ "        \"name\" : {\n"
				+ "          \"U\" : \"Alice\"\n"
				+ "        }\n"
				+ "      }\n"
				+ "    }\n"
				+ "  }\n"
				+ "}");
		}

		@Test
		void shouldSerializeNodes(@Mock Node node) throws JsonProcessingException {

			when(node.getId()).thenReturn(4711L);
			when(node.getLabels()).thenReturn(List.of(Label.label("A"), Label.label("B")));
			when(node.getAllProperties()).thenReturn(new TreeMap<>(Map.of("prop1", 1, "prop2", "Peng")));
			var result = objectMapper.writeValueAsString(node);
			assertThat(result).isEqualTo(
				"{\"()\":{\"id\":{\"Z\":\"4711\"},\"labels\":{\"[]\":[{\"U\":\"A\"},{\"U\":\"B\"}]},\"properties\":{\"{}\":{\"prop1\":{\"Z\":\"1\"},\"prop2\":{\"U\":\"Peng\"}}}}}");
		}

		@Test
		void shouldSerializeNodesWithoutLabels(@Mock Node node) throws JsonProcessingException {

			when(node.getId()).thenReturn(4711L);
			when(node.getLabels()).thenReturn(List.of());
			var result = objectMapper.writeValueAsString(node);
			assertThat(result)
				.isEqualTo("{\"()\":{\"id\":{\"Z\":\"4711\"},\"labels\":{\"[]\":[]},\"properties\":{\"{}\":{}}}}");
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
					"{\"->\":{\"id\":{\"Z\":\"4711\"},\"type\":{\"U\":\"KNOWS\"},\"startNodeId\":{\"Z\":\"123\"},\"endNodeId\":{\"Z\":\"124\"},\"properties\":{\"{}\":{\"since\":{\"Z\":\"1999\"}}}}}");

		}
	}
}
