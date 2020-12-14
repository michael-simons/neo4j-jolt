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

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

enum Sigil {
	INTEGER("Z"),
	REAL("R"),
	UNICODE("U"),
	BINARY("#"),
	LIST("[]"),
	MAP("{}"),
	TIME("T"),
	SPATIAL("@"),
	NODE("()"),
	RELATIONSHIP("->"),
	RELATIONSHIP_REVERSED("<-"),
	PATH(".."),
	BOOLEAN("?"),
	NULL("");

	Sigil(String value) {
		this.value = value;
	}

	private final String value;

	public String getValue() {
		return value;
	}

	private final static Map<String, Sigil> REVERSE_LOOKUP = Arrays.stream(Sigil.values())
		.collect(Collectors.toUnmodifiableMap(Sigil::getValue, Function.identity()));

	static Sigil ofLiteral(String value) {
		if (!REVERSE_LOOKUP.containsKey(value)) {
			throw new IllegalArgumentException(String.format("No Sigil with value '%s'.", value));
		}
		return REVERSE_LOOKUP.get(value);
	}
}
