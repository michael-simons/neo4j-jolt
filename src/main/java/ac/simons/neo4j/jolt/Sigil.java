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

import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.DurationValue;

enum Sigil {

	INTEGER("Z", Integer.class),
	REAL("R", Number.class, Long.class, Double.class),
	UNICODE("U", String.class),
	BINARY("#", byte[].class),
	LIST("[]", List.class),
	MAP("{}", Map.class),
	TIME("T", Temporal.class),
	TEMPORAL_AMOUNT("TA", TIME, TemporalAmount.class),
	SPATIAL("@", Point.class),
	NODE("()", Node.class),
	RELATIONSHIP("->", Relationship.class),
	RELATIONSHIP_REVERSED("<-", JoltRelationship.class),
	PATH("..", Path.class),
	BOOLEAN("?", Boolean.class),
	NULL("", Void.class);

	private final static Map<String, Sigil> REVERSE_LOOKUP = Arrays.stream(Sigil.values())
		.collect(Collectors.toUnmodifiableMap(Sigil::getValue, Function.identity()));

	private final String value;

	private final Sigil aliasFor;

	private final Class<?>[] types;

	Sigil(String value, Class... types) {
		this.value = value;
		this.types = types;
		this.aliasFor = null;
	}

	Sigil(String value, Sigil aliasFor, Class... types) {
		this.value = value;
		this.aliasFor = aliasFor;
		this.types = types;
	}

	String getValue() {
		return this.value;
	}

	String getAliasedValueOrValue() {
		return this.aliasFor == null ? this.value : this.aliasFor.value;
	}

	Class<?>[] getTypes() {
		return types;
	}

	static Sigil ofLiteral(String value) {
		if (!REVERSE_LOOKUP.containsKey(value)) {
			throw new IllegalArgumentException(String.format("No Sigil with value '%s'.", value));
		}
		return REVERSE_LOOKUP.get(value);
	}

	static Sigil forType(Class<?> type) {

		if (type == null) {
			return Sigil.NULL;
		}

		for (Sigil sigil : Sigil.values()) {
			for (Class<?> supportedType : sigil.types) {
				if (supportedType.isAssignableFrom(type)) {
					return sigil;
				}
			}
		}

		throw new IllegalArgumentException(type + " is not a supported type");
	}
}
