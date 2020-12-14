package ac.simons.neo4j.jolt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JoltDeserializerTest {

	private final ObjectMapper objectMapper;

	JoltDeserializerTest() {

		this.objectMapper = new ObjectMapper();
		this.objectMapper.registerModule(JoltModule.STRICT.getInstance());
	}

	@Nested
	class JoltDelegatingValueDeserializerTest {

		@Test
		void shouldRequireStructOnStrictMode() {
			assertThatExceptionOfType(JsonProcessingException.class)
				.isThrownBy(() -> objectMapper.readValue("123", Integer.class))
				.withMessageMatching("(?s)The provided value is not compatible with Jolt in strict mode:.*");
		}

		@Test
		void shouldRequireCorrectSigil() {
			assertThatExceptionOfType(JsonProcessingException.class)
				.isThrownBy(() -> objectMapper.readValue("{\"Ä\":\"123\"}", Integer.class))
				.withMessageMatching("(?s)Jolt does not support a named datatype 'Ä':.*");
		}

		@Test
		void shouldMatchSigil() {
			assertThatExceptionOfType(JsonProcessingException.class)
				.isThrownBy(() -> objectMapper.readValue("{\"R\":\"123\"}", Integer.class))
				.withMessageMatching("(?s)Cannot deserialize a value of type REAL into INTEGER:.*");
		}

		@Test
		void shouldRequireStringValue() {
			assertThatExceptionOfType(JsonProcessingException.class)
				.isThrownBy(() -> objectMapper.readValue("{\"Z\":123}", Integer.class))
				.withMessageMatching("(?s)Only String values will be parsed in strict mode:.*");
		}

		@Test
		void shouldDealWithPrimitivesAndNulLValues() {
			var expectedMessage = "(?s)Requested value class is 'int' but the value is null or blank:.*";
			assertThatExceptionOfType(JsonProcessingException.class)
				.isThrownBy(() -> objectMapper.readValue("{\"Z\":\"\"}", int.class))
				.withMessageMatching(expectedMessage);

			assertThatExceptionOfType(JsonProcessingException.class)
				.isThrownBy(() -> objectMapper.readValue("{\"Z\":\" \\t \"}", int.class))
				.withMessageMatching(expectedMessage);
		}
	}

	@Nested
	class SimpleTypes {

		@Test
		void shouldDeserializeInteger() throws JsonProcessingException {

			var result = objectMapper.readValue("{\"Z\":\"123\"}", Integer.class);
			assertThat(result).isEqualTo(123);
		}

		@Test
		void shouldDeserializeBoolean() throws JsonProcessingException {

			var result = objectMapper.readValue("{\"?\":\"true\"}", Boolean.class);
			assertThat(result).isTrue();
		}

		@Test
		void shouldDeserializeLong() throws JsonProcessingException {

			var result = objectMapper.readValue("{\"R\":\"123\"}", Number.class);
			assertThat(result).isInstanceOf(Long.class).isEqualTo(123L);
		}

		@Test
		void shouldDeserializeDouble() throws JsonProcessingException {

			var result = objectMapper.readValue("{\"R\":\"42.23\"}", Number.class);
			assertThat(result).isInstanceOf(Double.class).isEqualTo(42.23);
		}

		@Test
		void shouldDeserializeString() throws JsonProcessingException {

			var result = objectMapper.readValue("{\"U\":\"Hello, World\"}", String.class);
			assertThat(result).isEqualTo("Hello, World");
		}

		@Test
		void shouldDeserializePoint() throws JsonProcessingException {

			var point = Values.pointValue(CoordinateReferenceSystem.WGS84, 12.994823, 55.612191);
			var result = objectMapper.readValue("{\"@\":\"SRID=4326;POINT(12.994823 55.612191)\"}", Point.class);
			assertThat(result).isEqualTo(point);
		}
	}

	@Nested
	class Arrays {

		@Test
		void shouldDeserializeLongArray() throws JsonProcessingException {

			var result = objectMapper.readValue("[{\"Z\":\"0\"},{\"Z\":\"1\"},{\"Z\":\"2\"}]", Long[].class);
			assertThat(result).isEqualTo(new Long[] { 0L, 1L, 2L });
		}

		@Test
		void shouldDeserializeByteArray() throws JsonProcessingException {

			var result = objectMapper
				.readValue("{\"#\":\"0001020304050608090A0B0C0D0E0F10\"}", byte[].class);
			assertThat(result).isEqualTo(new byte[] { 0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16 });
		}
	}

	@Nested
	@TestInstance(TestInstance.Lifecycle.PER_CLASS)
	class TemporalTypes {

		Stream<Arguments> temporals() {
			return Stream.of(
				Arguments.of("{\"T\":\"2020-12-14\"}", LocalDate.of(2020, 12, 14)),
				Arguments.of("{\"T\":\"21:21:00+04:00\"}", OffsetTime.of(LocalTime.of(21, 21, 0), ZoneOffset.ofHours(4))),
				Arguments.of("{\"T\":\"21:21:00\"}", LocalTime.of(21, 21, 0)),
				Arguments.of("{\"T\":\"2020-12-14T17:14:00+01:00[Europe/Berlin]\"}",
					ZonedDateTime.of(
					LocalDate.of(2020, 12, 14), LocalTime.of(17, 14, 0), ZoneId.of("Europe/Berlin"))),
				Arguments.of("{\"T\":\"2020-12-14T17:14:00\"}",
					LocalDateTime.of(LocalDate.of(2020, 12, 14), LocalTime.of(17, 14, 0)))
			);
		}

		@ParameterizedTest
		@MethodSource("temporals")
		void shouldDeserializeTemporals(String t, Temporal expectedValue) throws JsonProcessingException {

			var result = objectMapper.readValue(t, expectedValue.getClass());
			assertThat(result).isEqualTo(expectedValue);
		}

		@Test
		void shouldDeserializeDurationValue() throws JsonProcessingException {

			var result = objectMapper.readValue(String.format("{\"T\":\"%s\"}", "PT23H21M"), DurationValue.class);
			assertThat(result).isEqualTo(DurationValue.duration(Duration.ofHours(23).plusMinutes(21)));

			result = objectMapper.readValue(String.format("{\"T\":\"%s\"}", "P42D"), DurationValue.class);
			assertThat(result).isEqualTo(DurationValue.duration(Period.ofDays(42)));
		}
	}

	@Nested
	class Collections {

		@Test
		void shouldDeserializeArrays() throws JsonProcessingException {

			var result = objectMapper.readValue("[{\"U\":\"A\"},{\"U\":\"B\"}]", String[].class);
			assertThat(result).isEqualTo(new String[] { "A", "B" });
		}

		@Test
		void shouldDeserializeHomogenousList() throws JsonProcessingException {

			var result = objectMapper.readValue("{\"[]\":[{\"Z\":\"1\"},{\"Z\":\"2\"},{\"Z\":\"3\"}]}", List.class);
			assertThat(result).isEqualTo(List.of(1, 2, 3));
		}

		@Test
		void shouldDeserializeHeterogeneousList() throws JsonProcessingException {

			var result = objectMapper.readValue("{\"[]\":[{\"U\":\"A\"},{\"Z\":\"21\"},{\"R\":\"42.3\"}]}", List.class);
			assertThat(result).isEqualTo(List.of("A", 21, 42.3));
		}

		@Test
		void shouldDeserializeMap() throws JsonProcessingException {

			// Treemap only created to have a stable iterator for a non flaky test ;)
			var result = objectMapper.readValue("{\"{}\":{\"age\":{\"Z\":\"33\"},\"name\":{\"U\":\"Alice\"}}}", Map.class);
			assertThat(result).isEqualTo(new TreeMap<>(Map.of("name", "Alice", "age", 33)));
		}
	}
}
