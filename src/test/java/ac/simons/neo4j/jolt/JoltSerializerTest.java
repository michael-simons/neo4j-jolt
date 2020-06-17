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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JoltSerializerTest {

	private final ObjectMapper objectMapper;

	JoltSerializerTest() {

		this.objectMapper = new ObjectMapper();
		this.objectMapper.registerModule(new JoltModule());
	}

	@Nested
	class SimpleTypes {

		@Test
		void shouldSerializeInteger() throws JsonProcessingException {

			var result = objectMapper.writeValueAsString(123);
			assertThat(result).isEqualTo("{\"Z\":\"123\"}");
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
		void shouldSerializeByte() throws JsonProcessingException {

			var result = objectMapper.writeValueAsString((byte) 10);
			assertThat(result).isEqualTo("{\"#\":\"0A\"}");
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
}
