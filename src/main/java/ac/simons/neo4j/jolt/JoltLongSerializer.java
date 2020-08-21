package ac.simons.neo4j.jolt;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * A dedicated long serializer is needed to handle writing the correct sigil depending on the value.
 * If the long value is inside the Int32 range we write a Int sigil, otherwise we use the Real sigil.
 * @param <T> long or Long
 */
public class JoltLongSerializer<T> extends StdSerializer<T>
{
    public JoltLongSerializer( Class<T> t )
    {
        super(t);
    }

    @Override
    public void serialize( T value, JsonGenerator generator, SerializerProvider provider ) throws IOException
    {
        generator.writeStartObject(value);

        long longValue = (long) value;

        if ( longValue >= Integer.MIN_VALUE && longValue < Integer.MAX_VALUE )
        {
            generator.writeFieldName( Sigil.INTEGER.getValue() );
        }
        else
        {
            generator.writeFieldName( Sigil.REAL.getValue() );
        }

        generator.writeString(String.valueOf( longValue ));
        generator.writeEndObject();
    }
}
