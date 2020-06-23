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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.server.http.cypher.format.api.RecordEvent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

class JoltIT {

	private static Neo4j neo4j;
	private static ObjectMapper objectMapper;

	static {
		objectMapper = new ObjectMapper();
		objectMapper.registerModule(JoltModule.DEFAULT.getInstance());
	}

	@BeforeAll
	static void startNeo4j() {

		neo4j = Neo4jBuilders
			.newInProcessBuilder()
			.withDisabledServer()
			.build();
	}

	@Test
	void simpleTypes() throws JsonProcessingException {

		try (var tx = neo4j.defaultDatabaseService().beginTx()) {
			var result = tx.execute("UNWIND range(1,1,1) as n RETURN false, 'aString', 123, 14.4, 2147483649");
			// This is what the server JSON code would do...
			result.accept(row ->
			{
				var writer = objectMapper.writerWithDefaultPrettyPrinter();
				var formattedRow = writer.writeValueAsString(new RecordEvent(result.columns(), row::get));
				System.out.println(formattedRow);
				return true;
			});
		}
	}

	@Test
	void node() throws JsonProcessingException {

		try (var tx = neo4j.defaultDatabaseService().beginTx()) {
			var result = tx.execute("CREATE (n:MyLabel {aNumber: 1234, aLargeNumber: 2147483649, aFloat: 3.7, aString: 'a string', aDate: date('2015-07-21')," +
									" arrayOfStrings: ['s1', 's2'], arrayOfDates: [date('2015-07-29'), date('2015-07-30')]," +
									" aPoint: point({ latitude:toFloat('13.43'), longitude:toFloat('56.21')})}) RETURN n as awesomeNode");
			// This is what the server JSON code would do...
			result.accept(row ->
			{
				var writer = objectMapper.writerWithDefaultPrettyPrinter();
				var formattedRow = writer.writeValueAsString(new RecordEvent(result.columns(), row::get));
				System.out.println(formattedRow);
				return true;
			});
		}
	}

	@Test
	void path() throws JsonProcessingException {

		try (var tx = neo4j.defaultDatabaseService().beginTx()) {

			tx.execute( "CREATE (bob:Person {name: 'Bob James', age: 30})-[:KNOWS {since: 1978}]->(alice:Person {name: 'Alice Smith'})<-[:KNOWS]-(charlie:Person {name: 'Charlie Jackson'})" );
			var result = tx.execute("MATCH (bob:Person {name: 'Bob James'}),(charlie:Person {name: 'Charlie Jackson'}), p = shortestPath((bob)-[*..15]-(charlie)) RETURN p AS awesomePath");
			// This is what the server JSON code would do...
			result.accept(row ->
						  {
							  var writer = objectMapper.writerWithDefaultPrettyPrinter();
							  var formattedRow = writer.writeValueAsString(new RecordEvent(result.columns(), row::get));
							  System.out.println(formattedRow);
							  return true;
						  });
		}
	}

	@AfterAll
	static void stopNeo4j() {

		neo4j.close();
	}
}
