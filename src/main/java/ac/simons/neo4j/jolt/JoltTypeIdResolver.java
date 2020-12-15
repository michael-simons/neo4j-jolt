package ac.simons.neo4j.jolt;

import java.io.IOException;
import java.time.temporal.Temporal;

import org.neo4j.values.storable.DurationValue;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.type.TypeFactory;

class JoltTypeIdResolver extends TypeIdResolverBase {

	@Override
	public String idFromValue(Object value) {

		if (value == null) {
			return Sigil.NULL.getValue();
		}
		return checkForLongForRealz(Sigil.forType(value.getClass()), value);
	}

	@Override
	public String idFromValueAndType(Object value, Class<?> suggestedType) {
		if (suggestedType == null) {
			return idFromValue(value);
		}
		return checkForLongForRealz(Sigil.forType(suggestedType), value);
	}

	protected String checkForLongForRealz(Sigil sigil, Object value) {
		if (sigil == Sigil.REAL && value instanceof Long) {
			Long longValue = (Long) value;
			if (longValue >= Integer.MIN_VALUE && longValue < Integer.MAX_VALUE) {
				return Sigil.INTEGER.getValue();
			}
		}
		return sigil.getAliasedValueOrValue();
	}

	@Override
	public JsonTypeInfo.Id getMechanism() {
		return JsonTypeInfo.Id.CUSTOM;
	}

	@Override
	public JavaType typeFromId(DatabindContext context, String id) throws IOException {

		var sigil = Sigil.ofLiteral(id);
		if (sigil == Sigil.REAL) {
			return TypeFactory.defaultInstance().constructType(Number.class);
		}
		if (sigil == Sigil.TIME && context instanceof DeserializationContext) {
			var deserializationContext = (DeserializationContext) context;

			if (false && deserializationContext.getParser().nextToken() == JsonToken.VALUE_STRING) {

				var text = deserializationContext.getParser().getText();
				if (text.matches("[-+]?P")) {
					return TypeFactory.defaultInstance().constructType(DurationValue.class);
				}
			}
			return TypeFactory.defaultInstance().constructType(Temporal.class);
		}

		return TypeFactory.defaultInstance().constructType(sigil.getTypes()[0]);
	}
}
