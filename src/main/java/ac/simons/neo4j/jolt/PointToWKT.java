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

import static java.util.stream.Collectors.*;

import java.util.function.Function;

import org.neo4j.graphdb.spatial.Point;

final class PointToWKT implements Function<Point, String> {

	@Override
	public String apply(Point point) {

		var coordinates = point.getCoordinate().getCoordinate();
		var wkt = new StringBuilder()
			.append("SRID=")
			.append(point.getCRS().getCode())
			.append(";POINT")
			.append(coordinates.size() == 3 ? " Z " : "")
			.append("(")
			.append(coordinates.stream().map(String::valueOf).collect(joining(" ")))
			.append(")");
		return wkt.toString();
	}
}
