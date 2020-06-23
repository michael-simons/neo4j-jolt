package ac.simons.neo4j.jolt;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

public class JoltPathSerializer extends StdSerializer<Path>
{
    JoltPathSerializer()
    {
        super( Path.class );
    }

    @Override
    public void serialize( Path path, JsonGenerator generator, SerializerProvider provider ) throws IOException
    {
        generator.writeStartObject( path );
        generator.writeFieldName( Sigil.PATH.getValue() );

        generator.writeStartArray();

        var it = path.iterator();
        var lastNodeId = 0L;

        while ( it.hasNext() )
        {
            var entity = it.next();
            if ( entity instanceof Node )
            {
                Node node = (Node) entity;
                lastNodeId = node.getId();

                generator.writeObject( node );
            }
            else if ( entity instanceof Relationship )
            {
                Relationship rel = (Relationship) entity;

                if ( rel.getStartNodeId() != lastNodeId )
                {
                    // we want a reversed relationship here so the path flows correctly
                    generator.writeObject( JoltRelationship.fromRelationshipReversed( rel ) );
                }
                else
                {
                    generator.writeObject( rel );
                }
            }
        }

        generator.writeEndArray();

        generator.writeEndObject();
    }
}
