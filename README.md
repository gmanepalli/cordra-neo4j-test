# Cordra Neo4j Extension
The original extension from CNRI registers each Cordra object as one node in Neo4j and establishes one-to-one relationship between the nodes if there any links between the corresponding Cordra objects.

However, sometimes you want to express **simultaneous** relationship with multiple Cordra objects. In JSON, you express this typically using a JSON property, whose value is a JSON object and multiple 
outgoing links flow out from this JSON object. It would be appropriate to register this inner JSON object as its own node in Neo4j and establish relationships from this node to the other nodes
it links.

Likewise, sometimes you want to explore the relationship hierarchy inherent to a JSON such as JSON objects within JSON objects or arrays of JSON objects. A Neo4j setup that mimics this tree structure allows for powerful querying and analysis of the data.

Finally, it is idomatic to use SNAKE_CASE for relationship types and PascalCase for node labels in Neo4j.

This extension improves the original extension by handling the above scenarios. In particular:

1. The JSON from a Cordra object is denormalized (flattened into a tree of JSON objects and arrays of JSON objects) and registered as nodes and relationships in Neo4j.
    * When the property value is a primitive or an array of primitives, it is stored as an attribute on the current node.
    * When the property value is a JSON object, a Neo4j node is created for this JSON object and relationship from the parent to this new node is created.
    * When the property value is an array of JSON objects, new Neo4j nodes are created - one for each array entry - and one relationship from the parent to each new node is created.

2. Node labels and relationship types can be expressed in the JSON schema registered with Cordra. It will default to the property name for the relationship type.
    * If the property links to a Cordra object, relationship type can be expressed as a JSON config on this property.
    * If the property value is a JSON object, node labels and relationship types can be expressed as a JSON config on this property.
    * If the property value is a JSON array of JSON objects, the relationship type can be expressed on the property whose type is "array" and the node label can be expressed on the item property whose type is "object".

3. propertyNameMode configuration of the extension is now moot.

## Configuration

You can set the node label on the JSON schema registered with Cordra like so:

```json
{
    "Cordra": {
        "ext": {
            "neo4j": {
                "nodeLabel": "NewNodeLabel"
            }
        }
    }
}
```

You can set the relationship type on the JSON schema like so:

```json
{
    "Cordra": {
        "ext": {
            "neo4j": {
                "relationshipType": "NEW_RELATIONSHIP_TYPE"
            }
        }
    }
}
```

You can do both, wherever meaningful.

## Final Thoughts

There are some scenarios that are not tested or handled. For instance, there is no way to configure certain properties of a JSON object to be properties on a Neo4j relationship. Likewise, arrays of heterogeneous types are not supported.

Feel free to reach out to me on twitter @neuroboom.

