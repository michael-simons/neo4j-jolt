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

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalQuery;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.spatial.Point;
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
                this.addSerializer( new JoltDelegatingValueSerializer<>( String.class, Function.identity() ) );

                this.addSerializer( new JoltDelegatingValueSerializer<>( boolean.class, String::valueOf ) );
                this.addSerializer( new JoltDelegatingValueSerializer<>( Boolean.class, String::valueOf ) );

                this.addSerializer( new JoltDelegatingValueSerializer<>( int.class, String::valueOf ) );
                this.addSerializer( new JoltDelegatingValueSerializer<>( Integer.class, String::valueOf ) );

                this.addSerializer( new JoltDelegatingValueSerializer<>( long.class, String::valueOf ) );
                this.addSerializer( new JoltDelegatingValueSerializer<>( Long.class, String::valueOf ) );

                this.addSerializer( new JoltDelegatingValueSerializer<>( Void.class, String::valueOf ) );
            }
            else
            {
                this.addSerializer( new JoltSparseNumberSerializer<>( int.class, Sigil.INTEGER, String::valueOf ) );
                this.addSerializer( new JoltSparseNumberSerializer<>( Integer.class, Sigil.INTEGER, String::valueOf ) );

                this.addSerializer( new JoltSparseNumberSerializer<>( long.class, Sigil.INTEGER, String::valueOf ) );
                this.addSerializer( new JoltSparseNumberSerializer<>( Long.class, Sigil.INTEGER, String::valueOf ) );
            }

            this.addSerializer( new JoltDelegatingValueSerializer<>( double.class, String::valueOf ) );
            this.addSerializer( new JoltDelegatingValueSerializer<>( Double.class, String::valueOf ) );

            this.addSerializer( new JoltDelegatingValueSerializer<>( byte[].class, JoltModuleImpl::toHexString ) );

            this.addSerializer( new JoltDelegatingValueSerializer<>( Point.class, new PointToWKT() ) );

            this.addSerializer( new JoltDelegatingValueSerializer<>( LocalDate.class, DateTimeFormatter.ISO_LOCAL_DATE::format ) );
            this.addSerializer( new JoltDelegatingValueSerializer<>( OffsetTime.class, DateTimeFormatter.ISO_OFFSET_TIME::format ) );
            this.addSerializer( new JoltDelegatingValueSerializer<>( LocalTime.class, DateTimeFormatter.ISO_LOCAL_TIME::format ) );
            this.addSerializer( new JoltDelegatingValueSerializer<>( ZonedDateTime.class, DateTimeFormatter.ISO_ZONED_DATE_TIME::format ) );
            this.addSerializer( new JoltDelegatingValueSerializer<>( LocalDateTime.class, DateTimeFormatter.ISO_LOCAL_DATE_TIME::format ) );
            this.addSerializer( new JoltDelegatingValueSerializer<>( DurationValue.class, DurationValue::toString ) );

            this.addSerializer( new StdDelegatingSerializer( Label.class, new JoltLabelConverter() ) );
            this.addSerializer(
                new StdDelegatingSerializer( RelationshipType.class, new JoltRelationshipTypeConverter() ) );

            this.addSerializer( new JoltNodeSerializer() );
            this.addSerializer( new JoltRelationshipSerializer() );
            this.addSerializer( new JoltRelationshipReversedSerializer() );
            this.addSerializer( new JoltPathSerializer() );
        }

        private void addDeserializers( boolean strictModeEnabled )
        {

            if ( strictModeEnabled )
            {
                this.addDeserializer(String.class, new JoltDelegatingValueDeserializer<>( String.class, Function.identity() ) );

                this.addDeserializer(boolean.class, new JoltDelegatingValueDeserializer<>( boolean.class, Boolean::parseBoolean ) );
                this.addDeserializer(Boolean.class, new JoltDelegatingValueDeserializer<>( Boolean.class, Boolean::valueOf ) );

                this.addDeserializer(int.class, new JoltDelegatingValueDeserializer<>( int.class, Integer::parseInt ) );
                this.addDeserializer(Integer.class, new JoltDelegatingValueDeserializer<>( Integer.class, Integer::valueOf ) );

                this.addDeserializer( long.class, new JoltDelegatingValueDeserializer<>( long.class, Long::parseLong ) );
                this.addDeserializer( Long.class, new JoltDelegatingValueDeserializer<>( Long.class, Long::valueOf ) );
            }
            else
            {
                // TODO Unsure…?
            }

            this.addDeserializer(Number.class, new JoltDelegatingValueDeserializer<>( Number.class,  v -> {

                // Probably not the best place…
                try {
                    return Long.parseLong(v);
                } catch (NumberFormatException e) {
                    return Double.parseDouble(v);
                }
            } ));

            this.addDeserializer( byte[].class, new JoltDelegatingValueDeserializer<>( byte[].class, JoltModuleImpl::fromHexString ) );

            this.addDeserializer( Point.class, new JoltDelegatingValueDeserializer<>( Point.class, new WKTToPoint() ) );

            // See above, this is Kraut und Rüben!
            var supportedTemporals =
                Map.<DateTimeFormatter, TemporalQuery<Temporal>>of(
                    DateTimeFormatter.ISO_LOCAL_DATE, LocalDate::from,
                    DateTimeFormatter.ISO_OFFSET_TIME, OffsetTime::from,
                    DateTimeFormatter.ISO_LOCAL_TIME, LocalTime::from,
                    DateTimeFormatter.ISO_ZONED_DATE_TIME, ZonedDateTime::from,
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME, LocalDateTime::from
                );
            this.addDeserializer(Temporal.class, new JoltDelegatingValueDeserializer<>(Temporal.class,
                v -> {
                    for (var x : supportedTemporals.entrySet()) {
                        try {
                            return x.getKey().parse(v, x.getValue());
                        } catch (DateTimeParseException e) {
                        }
                    }
                    throw new DateTimeException("No parser could handle the given value " + v);
                }));
            this.addDeserializer(TemporalAmount.class, new JoltDelegatingValueDeserializer<>(TemporalAmount.class, DurationValue::parse));
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
