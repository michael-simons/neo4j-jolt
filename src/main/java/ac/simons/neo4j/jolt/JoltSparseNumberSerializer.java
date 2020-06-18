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
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

/**
 * Omits the number sigil if the value is in integer range.
 *
 * @param <T>
 */
final class JoltSparseNumberSerializer<T extends Number> extends StdSerializer<T> {

	private final JsonSerializer<T> delegate;
	private final Sigil sigil;
	private final Function<T, String> converter;

	JoltSparseNumberSerializer(Class<T> t, Sigil sigil, Function<T, String> converter) {
		super(t);
		this.sigil = sigil;
		this.converter = converter;

		this.delegate = new JoltDelegatingValueSerializer<>(t, sigil, converter);
	}

	@Override
	public void serialize(T value, JsonGenerator generator, SerializerProvider provider) throws IOException {

		long longValue = value.longValue();
		if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
			generator.writeNumber(longValue);
		} else {
			delegate.serialize(value, generator, provider);
		}
	}
}
