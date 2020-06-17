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

import java.util.function.Function;

import com.fasterxml.jackson.databind.module.SimpleModule;

public class JoltModule extends SimpleModule {

	private static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();

	public JoltModule() {

		addSerializer(Integer.class,
			new JoltDelegatingValueSerializer<>(Integer.class, Sigil.INTEGER, String::valueOf));
		addSerializer(Long.class, new JoltDelegatingValueSerializer<>(Long.class, Sigil.INTEGER, String::valueOf));
		addSerializer(Double.class, new JoltDelegatingValueSerializer<>(Double.class, Sigil.REAL, String::valueOf));
		addSerializer(String.class,
			new JoltDelegatingValueSerializer<>(String.class, Sigil.UNICODE, Function.identity()));
		addSerializer(byte.class,
			new JoltDelegatingValueSerializer<>(byte.class, Sigil.BYTE, b -> toHexString(new byte[] { b })));
		addSerializer(Byte.class,
			new JoltDelegatingValueSerializer<>(Byte.class, Sigil.BYTE, b -> toHexString(new byte[] { b })));
		addSerializer(byte[].class,
			new JoltDelegatingValueSerializer<>(byte[].class, Sigil.BYTE, JoltModule::toHexString));

		var listSerializer = new JoltListSerializer();
		addSerializer(listSerializer.handledType(), listSerializer);
		var mapSerializer = new JoltMapSerializer();
		addSerializer(mapSerializer.handledType(), mapSerializer);
	}

	private static String toHexString(byte[] bytes) {
		var sb = new StringBuilder(2 * bytes.length);
		for (var b : bytes) {
			sb.append(HEX_DIGITS[(b >> 4) & 0xf]).append(HEX_DIGITS[b & 0xf]);
		}
		return sb.toString();
	}

}
