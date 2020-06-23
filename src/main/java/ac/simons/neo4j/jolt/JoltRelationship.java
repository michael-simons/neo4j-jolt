package ac.simons.neo4j.jolt;

import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class JoltRelationship
{
    private final long id;
    private final long startNodeId;
    private final long endNodeId;
    private final RelationshipType relationshipType;
    private final Map<String,Object> properties;

    private JoltRelationship(long id, long startNodeId, RelationshipType relationshipType, long endNodeId,  Map<String, Object> properties )
    {
        this.id = id;
        this.startNodeId = startNodeId;
        this.endNodeId = endNodeId;
        this.relationshipType = relationshipType;
        this.properties = properties;
    }

    public static JoltRelationship fromRelationshipReversed( Relationship relationship )
    {
        return new JoltRelationship(relationship.getId(), relationship.getEndNodeId(),
                                    relationship.getType(), relationship.getStartNodeId(), relationship.getAllProperties());
    }

    public RelationshipType getType()
    {
        return relationshipType;
    }

    public long getId()
    {
        return id;
    }

    public long getStartNodeId()
    {
        return startNodeId;
    }

    public long getEndNodeId()
    {
        return endNodeId;
    }

    public Map<String,Object> getAllProperties()
    {
        return properties;
    }
}
