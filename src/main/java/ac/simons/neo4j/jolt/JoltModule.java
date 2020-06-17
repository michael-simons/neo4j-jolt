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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.spatial.Point;

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdDelegatingSerializer;

public final class JoltModule extends SimpleModule {

	private static volatile JoltModule INSTANCE;

	private static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();

	public static JoltModule getInstance() {

		JoltModule instance = JoltModule.INSTANCE;
		if (instance == null) {
			synchronized (JoltModule.class) {
				instance = JoltModule.INSTANCE;
				if (instance == null) {
					JoltModule.INSTANCE = new JoltModule();
					instance = JoltModule.INSTANCE;
				}
			}
		}
		return instance;
	}

	private JoltModule() {

		if (INSTANCE != null) {
			throw new IllegalStateException("JoltModule has already been created.");
		}

		this.addSerializer(new JoltDelegatingValueSerializer<>(Integer.class, Sigil.INTEGER, String::valueOf));
		this.addSerializer(new JoltDelegatingValueSerializer<>(Long.class, Sigil.INTEGER, String::valueOf));
		this.addSerializer(new JoltDelegatingValueSerializer<>(Double.class, Sigil.REAL, String::valueOf));
		this.addSerializer(new JoltDelegatingValueSerializer<>(String.class, Sigil.UNICODE, Function.identity()));
		this.addSerializer(new JoltDelegatingValueSerializer<>(byte[].class, Sigil.BINARY, JoltModule::toHexString));
		this.addSerializer(new JoltDelegatingValueSerializer<>(Point.class, Sigil.SPATIAL, new PointToWKT()));

		this.addSerializer(new JoltDelegatingValueSerializer<>(LocalDate.class, Sigil.TIME,
			DateTimeFormatter.ISO_LOCAL_DATE::format));
		this.addSerializer(new JoltDelegatingValueSerializer<>(OffsetTime.class, Sigil.TIME,
			DateTimeFormatter.ISO_OFFSET_TIME::format));
		this.addSerializer(new JoltDelegatingValueSerializer<>(LocalTime.class, Sigil.TIME,
			DateTimeFormatter.ISO_LOCAL_TIME::format));
		this.addSerializer(new JoltDelegatingValueSerializer<>(ZonedDateTime.class, Sigil.TIME,
			DateTimeFormatter.ISO_ZONED_DATE_TIME::format));
		this.addSerializer(new JoltDelegatingValueSerializer<>(LocalDateTime.class, Sigil.TIME,
			DateTimeFormatter.ISO_LOCAL_DATE_TIME::format));
		// TODO Duration missing

		this.addSerializer(new StdDelegatingSerializer(Label.class, new JoltLabelConverter()));
		this.addSerializer(new StdDelegatingSerializer(RelationshipType.class, new JoltRelationshipTypeConverter()));

		this.addSerializer(new JoltNodeSerializer());
		this.addSerializer(new JoltRelationshipSerializer());

		this.addSerializer(new JoltMapSerializer());
		this.addSerializer(new JoltListSerializer());

		this.addSerializer(new JoltRecordEventSerializer());
	}

	private static String toHexString(byte[] bytes) {
		var sb = new StringBuilder(2 * bytes.length);
		for (var b : bytes) {
			sb.append(HEX_DIGITS[(b >> 4) & 0xf]).append(HEX_DIGITS[b & 0xf]);
		}
		return sb.toString();
	}

}
