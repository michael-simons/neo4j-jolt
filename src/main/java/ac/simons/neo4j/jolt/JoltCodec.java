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

import java.io.IOException;
import java.util.Collection;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.util.JsonParserSequence;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsWrapperTypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;
import com.fasterxml.jackson.databind.util.TokenBuffer;

/**
 * Object Mapper configured to write results using the Jolt format.
 * Jolt typically produces results in the format: {@code {<type> : <value>} }.
 * For example: {@code {"Z": 1}} where "Z" indicates the value is an integer.
 */
public class JoltCodec extends ObjectMapper {
	/**
	 * Construct a codec with strict mode enabled/disabled depending on {@code strictModeEnabled}. When strict
	 * mode is enabled, values are <em>always</em> paired with their type whereas when disabled some type information
	 * is omitted for brevity.
	 *
	 * @param strictModeEnabled {@code true} to enable strict mode, {@code false} to disable strict mode.
	 */
	public JoltCodec(boolean strictModeEnabled) {
		if (strictModeEnabled) {
			registerModules(JoltModule.STRICT.getInstance());
		} else {
			registerModules(JoltModule.DEFAULT.getInstance());
		}

		StdTypeResolverBuilder resolver = new StdTypeResolverBuilder() {

			@Override public TypeSerializer buildTypeSerializer(SerializationConfig config, JavaType baseType,
				Collection<NamedType> subtypes) {
				if(baseType.isArrayType() && !baseType.isTypeOrSubTypeOf(byte[].class)) {
					return null;
				}
				return super.buildTypeSerializer(config, baseType, subtypes);
			}

			@Override
			public TypeDeserializer buildTypeDeserializer(DeserializationConfig config,
				JavaType baseType, Collection<NamedType> subtypes) {
				if ((baseType.isArrayType() && !baseType.isTypeOrSubTypeOf(byte[].class)) || baseType.isPrimitive()||_idType == JsonTypeInfo.Id.NONE) {
					return null;
				}

				final PolymorphicTypeValidator subTypeValidator = verifyBaseTypeValidity(config, baseType);
				TypeIdResolver idRes = idResolver(config, baseType, subTypeValidator, subtypes, false, true);
				JavaType defaultImpl = defineDefaultImpl(config, baseType);

				switch (_includeAs) {
					// Only thing we support and we need to "fix" it due to the fact that Jolt uses T for
					// both temporal amounts and actual temporals
					case WRAPPER_OBJECT:
						return new JoltAsWrapperTypeDeserializer(baseType, idRes, _typeProperty, _typeIdVisible, defaultImpl);
				}
				throw new IllegalStateException(
					"Do not know how to construct standard type serializer for inclusion type: " + _includeAs);
			}
		};
		resolver.init(JsonTypeInfo.Id.CUSTOM, new JoltTypeIdResolver());
		resolver.inclusion(JsonTypeInfo.As.WRAPPER_OBJECT);
		activateDefaultTyping(new PolymorphicTypeValidator.Base() {
			@Override public Validity validateBaseType(MapperConfig<?> config, JavaType baseType) {
				return Validity.ALLOWED;
			}
		}, DefaultTyping.EVERYTHING, JsonTypeInfo.As.WRAPPER_OBJECT);
		setDefaultTyping(resolver);
	}

	/**
	 * Constructs a codec with strict mode disabled.
	 */
	public JoltCodec() {
		this(false);
	}

	private static class JoltAsWrapperTypeDeserializer extends AsWrapperTypeDeserializer {

		public JoltAsWrapperTypeDeserializer(JavaType bt, TypeIdResolver idRes, String typePropertyName, boolean typeIdVisible, JavaType defaultImpl) {
			super(bt, idRes, typePropertyName, typeIdVisible, defaultImpl);
		}

		protected Object _deserialize(JsonParser p, DeserializationContext ctxt) throws
			IOException {
			try {
				return super._deserialize(p, ctxt);
			} catch (InvalidTypeIdException e) {

				// Try again with temporal amount
				if (Sigil.ofLiteral(p.getText()) == Sigil.TIME) {
					TokenBuffer tb = new TokenBuffer(p.getCodec(), false);
					tb.writeStartObject();
					tb.writeFieldName(Sigil.TEMPORAL_AMOUNT.getValue());
					p = JsonParserSequence.createFlattened(false, tb.asParser(p), p);
					p.nextToken();
					return super._deserialize(p, ctxt);
				}
				throw e;
			}
		}
	}
}
