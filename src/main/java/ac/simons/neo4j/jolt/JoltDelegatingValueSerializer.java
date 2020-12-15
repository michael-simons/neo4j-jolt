package ac.simons.neo4j.jolt;

import java.io.IOException;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;

public class JoltDelegatingValueSerializer<T> extends StdScalarSerializer<T> {
	private final Function<T, String> converter;

	JoltDelegatingValueSerializer(Class<T> t, Function<T, String> converter) {
		super(t);
		this.converter = converter;
	}

	@Override
	public void serialize(T value, JsonGenerator generator, SerializerProvider provider) throws IOException {
		generator.writeString(converter.apply(value));
	}
}
