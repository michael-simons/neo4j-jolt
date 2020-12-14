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

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Function;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.register.Register;
import org.neo4j.values.storable.DurationValue;

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdDelegatingSerializer;

enum JoltModule
{
    DEFAULT( new JoltModuleImpl( false ) ),
    STRICT( new JoltModuleImpl( true ) );

    private final SimpleModule instance;

    JoltModule( SimpleModule instance )
    {
        this.instance = instance;
    }

    public SimpleModule getInstance()
    {
        return instance;
    }

    private static class JoltModuleImpl extends SimpleModule
    {

        private static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();

        private JoltModuleImpl( boolean strictModeEnabled )
        {
           addSerializers(strictModeEnabled);
           addDeserializers(strictModeEnabled);
        }

        private void addSerializers( boolean strictModeEnabled )
        {

            if ( strictModeEnabled )
            {
                this.addSerializer( new JoltDelegatingValueSerializer<>( String.class, Sigil.UNICODE, Function.identity() ) );

                this.addSerializer( new JoltDelegatingValueSerializer<>( boolean.class, Sigil.BOOLEAN, String::valueOf ) );
                this.addSerializer( new JoltDelegatingValueSerializer<>( Boolean.class, Sigil.BOOLEAN, String::valueOf ) );

                this.addSerializer( new JoltDelegatingValueSerializer<>( int.class, Sigil.INTEGER, String::valueOf ) );
                this.addSerializer( new JoltDelegatingValueSerializer<>( Integer.class, Sigil.INTEGER, String::valueOf ) );

                this.addSerializer( new JoltLongSerializer<>( long.class ) );
                this.addSerializer( new JoltLongSerializer<>( Long.class ) );

                this.addSerializer( new JoltDelegatingValueSerializer<>( Void.class, Sigil.NULL, String::valueOf ) );
                this.addSerializer( new JoltListSerializer() );
            }
            else
            {
                this.addSerializer( new JoltSparseNumberSerializer<>( int.class, Sigil.INTEGER, String::valueOf ) );
                this.addSerializer( new JoltSparseNumberSerializer<>( Integer.class, Sigil.INTEGER, String::valueOf ) );

                this.addSerializer( new JoltSparseNumberSerializer<>( long.class, Sigil.INTEGER, String::valueOf ) );
                this.addSerializer( new JoltSparseNumberSerializer<>( Long.class, Sigil.INTEGER, String::valueOf ) );
            }

            this.addSerializer( new JoltDelegatingValueSerializer<>( double.class, Sigil.REAL, String::valueOf ) );
            this.addSerializer( new JoltDelegatingValueSerializer<>( Double.class, Sigil.REAL, String::valueOf ) );

            this.addSerializer( new JoltDelegatingValueSerializer<>( byte[].class, Sigil.BINARY, JoltModuleImpl::toHexString ) );

            this.addSerializer( new JoltDelegatingValueSerializer<>( Point.class, Sigil.SPATIAL, new PointToWKT() ) );

            this.addSerializer( new JoltDelegatingValueSerializer<>( LocalDate.class, Sigil.TIME,
                DateTimeFormatter.ISO_LOCAL_DATE::format ) );
            this.addSerializer( new JoltDelegatingValueSerializer<>( OffsetTime.class, Sigil.TIME,
                DateTimeFormatter.ISO_OFFSET_TIME::format ) );
            this.addSerializer( new JoltDelegatingValueSerializer<>( LocalTime.class, Sigil.TIME,
                DateTimeFormatter.ISO_LOCAL_TIME::format ) );
            this.addSerializer( new JoltDelegatingValueSerializer<>( ZonedDateTime.class, Sigil.TIME,
                DateTimeFormatter.ISO_ZONED_DATE_TIME::format ) );
            this.addSerializer( new JoltDelegatingValueSerializer<>( LocalDateTime.class, Sigil.TIME,
                DateTimeFormatter.ISO_LOCAL_DATE_TIME::format ) );
            this.addSerializer( new JoltDelegatingValueSerializer<>( DurationValue.class, Sigil.TIME,
                DurationValue::toString ) );

            this.addSerializer( new StdDelegatingSerializer( Label.class, new JoltLabelConverter() ) );
            this.addSerializer(
                new StdDelegatingSerializer( RelationshipType.class, new JoltRelationshipTypeConverter() ) );

            this.addSerializer( new JoltNodeSerializer() );
            this.addSerializer( new JoltRelationshipSerializer() );
            this.addSerializer( new JoltRelationshipReversedSerializer() );
            this.addSerializer( new JoltPathSerializer() );

            this.addSerializer( new JoltMapSerializer() );
        }

        private void addDeserializers( boolean strictModeEnabled )
        {

            if ( strictModeEnabled )
            {
                this.addDeserializer(String.class, new JoltDelegatingValueDeserializer<>( String.class, Sigil.UNICODE, Function.identity() ) );

                this.addDeserializer(boolean.class, new JoltDelegatingValueDeserializer<>( boolean.class, Sigil.BOOLEAN, Boolean::parseBoolean ) );
                this.addDeserializer(Boolean.class, new JoltDelegatingValueDeserializer<>( Boolean.class, Sigil.BOOLEAN, Boolean::valueOf ) );

                this.addDeserializer(int.class, new JoltDelegatingValueDeserializer<>( int.class, Sigil.INTEGER, Integer::parseInt ) );
                this.addDeserializer(Integer.class, new JoltDelegatingValueDeserializer<>( Integer.class, Sigil.INTEGER, Integer::valueOf ) );

                // Needed to support long[] arrays directly.
                this.addDeserializer(Long.class, new JoltDelegatingValueDeserializer<>( Long.class, Sigil.INTEGER, Long::valueOf ) );

                this.addDeserializer( List.class, new JoltListDeserializer() );
            }
            else
            {
                // TODO Unsure…?
            }

            this.addDeserializer( Number.class, new JoltRealDeserializer( ) );

            this.addDeserializer( byte[].class, new JoltDelegatingValueDeserializer<>( byte[].class, Sigil.BINARY, JoltModuleImpl::fromHexString ) );

            this.addDeserializer( Point.class, new JoltDelegatingValueDeserializer<>( Point.class, Sigil.SPATIAL, new WKTToPoint() ) );

            this.addDeserializer( LocalDate.class, new JoltDelegatingValueDeserializer<>( LocalDate.class, Sigil.TIME,
                v -> DateTimeFormatter.ISO_LOCAL_DATE.parse(v, LocalDate::from) ) );
            this.addDeserializer( OffsetTime.class, new JoltDelegatingValueDeserializer<>( OffsetTime.class, Sigil.TIME,
                v -> DateTimeFormatter.ISO_OFFSET_TIME.parse(v, OffsetTime::from) ) );
            this.addDeserializer( LocalTime.class, new JoltDelegatingValueDeserializer<>( LocalTime.class, Sigil.TIME,
                v -> DateTimeFormatter.ISO_LOCAL_TIME.parse(v, LocalTime::from) ) );
            this.addDeserializer( ZonedDateTime.class, new JoltDelegatingValueDeserializer<>( ZonedDateTime.class, Sigil.TIME,
                v -> DateTimeFormatter.ISO_ZONED_DATE_TIME.parse(v, ZonedDateTime::from) ) );
            this.addDeserializer( LocalDateTime.class, new JoltDelegatingValueDeserializer<>( LocalDateTime.class, Sigil.TIME,
                v -> DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(v, LocalDateTime::from) ) );
            this.addDeserializer( DurationValue.class, new JoltDelegatingValueDeserializer<>( DurationValue.class, Sigil.TIME,
                DurationValue::parse ) );
        }

        private static String toHexString( byte[] bytes )
        {
            var sb = new StringBuilder( 2 * bytes.length );
            for ( var b : bytes )
            {
                sb.append( HEX_DIGITS[(b >> 4) & 0xf] ).append( HEX_DIGITS[b & 0xf] );
            }
            return sb.toString();
        }

        private static byte[] fromHexString( String hexString )
        {
            var len = hexString.length();
            var data = new byte[len / 2];
            for (int i = 0; i < len; i += 2) {
                data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                                      + Character.digit(hexString.charAt(i+1), 16));
            }
            return data;
        }
    }
}
