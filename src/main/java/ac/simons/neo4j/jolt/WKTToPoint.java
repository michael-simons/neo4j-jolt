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

import java.util.function.Function;
import java.util.regex.Pattern;

import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

final class WKTToPoint implements Function<String, Point> {
	private final static Pattern WKT_PATTERN = Pattern.compile("SRID=(\\d+);\\s*POINT\\(\\s*(\\S+)\\s+(\\S+)\\s*\\)");

	@Override
	public Point apply(String value) {

	    var matcher = WKT_PATTERN.matcher(value);

		if (!matcher.matches()) {
			throw new IllegalArgumentException(String.format("Illegal %s value: %s", Sigil.SPATIAL, value));
		}

		return Values.pointValue(
		    CoordinateReferenceSystem.get(Integer.valueOf(matcher.group(1))),
            Double.parseDouble(matcher.group(2)),
            Double.parseDouble(matcher.group(3))
        );
	}
}
