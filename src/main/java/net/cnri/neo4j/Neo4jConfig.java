package net.cnri.neo4j;

import java.util.List;

public class Neo4jConfig {
    public String user;
    public String password;
    public String uri;

    public String databaseName;

    public List<String> includeTypes;

    public List<String> excludeTypes;

    public String propertyNameMode = "topLevel"; //topLevel or jsonPointer

    public boolean verbose = false;
}
