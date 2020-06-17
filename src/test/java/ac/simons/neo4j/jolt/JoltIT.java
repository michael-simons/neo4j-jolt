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
		objectMapper.registerModule(JoltModule.getInstance());
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
			var result = tx.execute("CREATE (n:MyLabel {aNumber: 1234, aLargeNumber: 2147483649, aFloat: 3.7, aString: 'a string', aDate: date('2015-07-21'), arrayOfStrings: ['s1', 's2'], arrayOfDates: [date('2015-07-29'), date('2015-07-30')], aPoint: point({ latitude:toFloat('13.43'), longitude:toFloat('56.21')})}) RETURN n as N");
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
