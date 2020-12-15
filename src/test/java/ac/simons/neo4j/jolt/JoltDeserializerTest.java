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
import java.time.temporal.TemporalAmount;
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

		this.objectMapper = new JoltCodec(true);
	}

	@Nested
	class SimpleTypes {

		@Test
		void shouldDeserializeInteger() throws JsonProcessingException {

			var result = objectMapper.readValue("{\"Z\":\"123\"}", Integer.class);
			assertThat(result).isInstanceOf(Integer.class).isEqualTo(123);
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
		void shouldDeserializeStringArray() throws JsonProcessingException {

			var result = objectMapper.readValue("[{\"U\":\"A\"},{\"U\":\"B\"}]", String[].class);
			assertThat(result).isEqualTo(new String[] { "A", "B" });
		}

		@Test
		void shouldDeserializeLongArray() throws JsonProcessingException {

			var result = objectMapper.readValue("[{\"R\":\"0\"},{\"R\":\"1\"},{\"R\":\"2\"}]", Number[].class);
			assertThat(result).isEqualTo(new Long[] { 0L, 1L, 2L });
		}

		@Test
		void shouldDeserializeMixedNumberArray() throws JsonProcessingException {

			var result = objectMapper.readValue("[{\"Z\":\"0\"},{\"R\":\"1\"},{\"Z\":\"2\"}]", Number[].class);
			assertThat(result).isEqualTo(new Number[] { 0, 1L, 2 });
		}

		@Test
		void shouldDeserializeIntegerArray() throws JsonProcessingException {

			var result = objectMapper.readValue("[{\"Z\":\"0\"},{\"Z\":\"1\"},{\"Z\":\"2\"}]", Integer[].class);
			assertThat(result).isEqualTo(new Integer[] { 0, 1, 2 });
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
				Arguments.of("{\"T\":\"2020-12-14\"}",     LocalDate.of(2020, 12, 14)),
				Arguments.of("{\"T\":\"21:21:00+04:00\"}", OffsetTime.of(LocalTime.of(21, 21, 0), ZoneOffset.ofHours(4))),
				Arguments.of("{\"T\":\"21:21:00\"}",       LocalTime.of(21, 21, 0)),
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

			var result = objectMapper.readValue(t, Temporal.class);
			assertThat(result).isInstanceOf(expectedValue.getClass()).isEqualTo(expectedValue);
		}

		@Test
		void shouldDeserializeDurationValue() throws JsonProcessingException {

			var result = objectMapper.readValue(String.format("{\"T\":\"%s\"}", "PT23H21M"), TemporalAmount.class);
			assertThat(result).isEqualTo(DurationValue.duration(Duration.ofHours(23).plusMinutes(21)));

			result = objectMapper.readValue(String.format("{\"T\":\"%s\"}", "P42D"), TemporalAmount.class);
			assertThat(result).isEqualTo(DurationValue.duration(Period.ofDays(42)));
		}
	}

	@Nested
	class Collections {



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
