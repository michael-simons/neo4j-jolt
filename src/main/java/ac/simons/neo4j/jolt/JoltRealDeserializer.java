package ac.simons.neo4j.jolt;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;

final class JoltRealDeserializer extends JoltStdDeserializer<Number> {

	JoltRealDeserializer() {
		super(Number.class, Sigil.REAL);
	}

	@Override
	Number deserializeImpl(JsonParser p, DeserializationContext ctxt) throws IOException {

		var valueAsString = getValueAsString(p);
		try {
			return Long.parseLong(valueAsString);
		} catch (NumberFormatException e) {
			return Double.parseDouble(valueAsString);
		}
	}
}
