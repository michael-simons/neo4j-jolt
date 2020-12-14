package ac.simons.neo4j.jolt;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;

abstract class JoltStdDeserializer<T> extends StdDeserializer<T> {

	protected final Sigil sigil;

	JoltStdDeserializer(Class<T> t, Sigil sigil) {
		super(t);
		this.sigil = sigil;
	}

	JoltStdDeserializer(JavaType valueType, Sigil sigil) {
		super(valueType);
		this.sigil = sigil;
	}

	@Override
	public final T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {

		var isStruct = p.getCurrentToken().isStructStart();
		if (!isStruct) {
			System.out.println(p.getCurrentToken().name()+ " " + p.currentName());
			throw new InvalidFormatException(p, "The provided value is not compatible with Jolt in strict mode:", null,
				super._valueClass);
		}

		Sigil requestedType = getSigilFromStruct(p);
		if (requestedType != sigil) {
			throw new InvalidFormatException(p,
				String.format("Cannot deserialize a value of type %s into %s:", requestedType, sigil), p.currentName(),
				super._valueClass);
		}

		var nextToken = p.nextToken();
		if(restrictToStringValues()) {
			if (nextToken != JsonToken.VALUE_STRING) {
				throw new InvalidFormatException(p, "Only String values will be parsed in strict mode:", nextToken,
					super._valueClass);
			}
		}

		T value = deserializeImpl(p, ctxt);
		if (!p.nextToken().isStructEnd()) {
			throw new InvalidFormatException(p, "Dangling object data found in a Jolt struct.", p.getCurrentToken(),
				super._valueClass);
		}
		return value;
	}

	/**
	 * @return True if only string values should be deserialized.
	 */
	protected boolean restrictToStringValues() {
		return true;
	}

	/**
	 * Extraacts the sigil and moves the field name forward.
	 * @param p
	 * @return
	 * @throws IOException
	 */
	final Sigil getSigilFromStruct(JsonParser p) throws IOException {

		var sigilLiteral = p.nextFieldName().trim();
		try {
			return Sigil.ofLiteral(sigilLiteral);
		} catch (IllegalArgumentException e) {
			throw new InvalidFormatException(p,
				String.format("Jolt does not support a named datatype '%s':", sigilLiteral), sigilLiteral,
				super._valueClass);
		}
	}

	final String getValueAsString(JsonParser p) throws IOException {

		var text = p.getText();
		var valueAsString = text == null ? "" : text.trim();
		if (super._valueClass.isPrimitive() && valueAsString.isBlank()) {
			throw new InvalidFormatException(p,
				String.format("Requested value class is '%s' but the value is null or blank:", _valueClass), valueAsString,
				super._valueClass);
		}

		return valueAsString;
	}

	abstract T deserializeImpl(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException;
}
