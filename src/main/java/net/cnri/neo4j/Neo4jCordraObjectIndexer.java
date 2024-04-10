package net.cnri.neo4j;

import com.exhypothesi.json.Json;
import com.google.gson.*;
import net.cnri.cordra.CordraHooksSupport;
import net.cnri.cordra.CordraHooksSupportProvider;
import net.cnri.cordra.api.*;

import net.cnri.cordra.util.GsonUtility;
import net.cnri.cordra.util.JsonUtil;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.neo4j.cypherdsl.core.*;
import org.neo4j.cypherdsl.core.renderer.Configuration;
import org.neo4j.cypherdsl.core.renderer.Renderer;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Neo4jCordraObjectIndexer {

    private static CordraHooksSupport hooks = CordraHooksSupportProvider.get();
    private static CordraClient cordra = hooks.getCordraClient();

    private volatile boolean shutdown = false;
    private static Neo4jCordraObjectIndexer instance;
    private volatile Driver driver;

    private volatile Neo4jConfig config;

    private Neo4jCordraObjectIndexer() throws CordraException {
        hooks.addShutdownHook(this::shutdown);
        loadConfigFromDesign();
    }

    public synchronized static Neo4jCordraObjectIndexer getInstance() throws CordraException {
        if (instance == null) {
            instance = new Neo4jCordraObjectIndexer();
        }
        return instance;
    }

    private SessionConfig getSessionConfig() {
        if (config.databaseName != null) {
            return SessionConfig.forDatabase(config.databaseName);
        } else {
            return SessionConfig.defaultConfig();
        }
    }

    public synchronized Neo4jConfig loadConfigFromDesign() throws CordraException {
        CordraObject designCo = cordra.get("design");
        Neo4jConfig configToLoad;
        if (designCo.getPayload("neo4jConfig") != null) {
            String configJson = readPayloadToString("design", "neo4jConfig");
            configToLoad = GsonUtility.getGson().fromJson(configJson, Neo4jConfig.class);
        } else {
            configToLoad = new Neo4jConfig();
            configToLoad.user = "neo4j";
            configToLoad.password = "password";
            configToLoad.uri = "bolt://localhost:7687";
        }
        return loadConfig(configToLoad);
    }

    public synchronized Neo4jConfig loadConfig(Neo4jConfig configToLoad) {
        if (driver != null) {
            driver.close();
        }
        AuthToken auth = AuthTokens.basic(configToLoad.user, configToLoad.password);
        this.driver = GraphDatabase.driver(configToLoad.uri, auth);
        this.config = configToLoad;
        return configToLoad;
    }

    private String readPayloadToString(String objectId, String payloadName) throws CordraException {
        try (InputStream in = cordra.getPayload(objectId, payloadName)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new InternalErrorCordraException(e);
        }
    }

    private boolean shouldIndexType(String type) {
        if (config.excludeTypes != null) {
            if (config.excludeTypes.contains(type)) {
                return false;
            }
        }
        if (config.includeTypes != null) {
            return config.includeTypes.contains(type);
        } else {
            //index everything
            return true;
        }
    }

    public void deleteAll() {
        deleteAll("CordraObject");
        deleteAll("_CordraObject");
    }

    public void deleteAll(String type) {
        Node match = Cypher.node(type)
                .named("matchNode");
        var statement = Cypher
                .match(match)
                .detachDelete(match)
                .build();

        String cypherQuery = Renderer
                .getRenderer(Configuration.prettyPrinting())
                .render(statement);
        logQuery(cypherQuery, config.verbose);

        try (Session session = driver.session(getSessionConfig())) {
            session.writeTransaction(tx -> {
                tx.run(cypherQuery);
                return null;
            });
        }
    }

    public JsonElement search(String cypherQuery) {
        JsonArray jsonRecords = new JsonArray();
        try (Session session = driver.session(getSessionConfig())) {
            Result result = session.run(cypherQuery);
            List<Record> records = result.list();
            Gson gson = GsonUtility.getPrettyGson();
            for (Record r : records) {
                Map<String, Object> map = r.asMap();
                JsonObject jsonRecord = gson.toJsonTree(map).getAsJsonObject();
                jsonRecords.add(jsonRecord);
            }
        }
        return jsonRecords;
    }

    public JsonElement reindexAll(boolean includeRelationships) throws CordraException {
        return reindexQueryResults("*:*", includeRelationships);
    }

    public JsonElement reindexQueryResults(String cordraQuery, boolean includeRelationships) throws CordraException {
        long count = 0;
        try (SearchResults<CordraObject> results = cordra.search(cordraQuery)) {
            count = reindex(results, includeRelationships);
        }
        JsonObject json = new JsonObject();
        json.addProperty("reindexCount", count);
        return json;
    }

    private long reindex(SearchResults<CordraObject> results, boolean includeRelationships) throws CordraException {
        long count = 0;
        for (CordraObject co : results) {
            Map<String, JsonElement> pointerToSchemaMap = hooks.getPointerToSchemaMap(co);
            if (shouldIndexType(co.type)) {
                update(co, pointerToSchemaMap, includeRelationships);
                count++;
            }
        }
        return count;
    }

    public JsonElement reindexId(String id, boolean includeRelationships) throws CordraException {
        CordraObject co = cordra.get(id);
        Map<String, JsonElement> pointerToSchemaMap = hooks.getPointerToSchemaMap(co);
        update(co, pointerToSchemaMap, includeRelationships);
        JsonObject json = new JsonObject();
        json.addProperty("reindexCount", 1);
        return json;
    }

    public void delete(CordraObject co) {
        var statement = buildDeleteStatement(co.id).build();

        String cypherQuery = Renderer
                .getRenderer(Configuration.prettyPrinting())
                .render(statement);
        logQuery(cypherQuery, config.verbose);

        try (Session session = driver.session(getSessionConfig())) {
            session.writeTransaction(tx -> {
                tx.run(cypherQuery);
                return null;
            });
        }
    }

    private static StatementBuilder.OngoingUpdate buildDeleteStatement(String id) {
        Node rootNode = Cypher.node("CordraObject")
                .named("root")
                .withProperties("_id", Cypher.literalOf(id));

        Node internalNode = Cypher.node("_CordraObject")
                .named("internal");

        Relationship internalRelationships = rootNode.relationshipTo(internalNode).unbounded();
        return Cypher.match(rootNode)
                .optionalMatch(internalRelationships)
                .detachDelete(internalNode)
                .with(rootNode)
                .detachDelete(rootNode);
    }

    public Value update(CordraObject co, Map<String, JsonElement> pointerToSchemaMap, boolean includeRelationships) {
        if (!shouldIndexType(co.type)) return null;

        /*
          MATCH (root:CordraObject {_id: 'test/93ba2d8d68a54657ef55'})
          SET root = $props
          OPTIONAL MATCH (root) -[*]-> (internal:`_CordraObject`)
          DETACH DELETE internal
          WITH root
          OPTIONAL MATCH (root) -[*]-> (external:CordraObject)
          DELETE e
          // build graph from here as if it were a create
         */

        // denest Json into non-nested objects and relationships
        Json denester = new Json(co.content);
        denester.denest();
        var pointerToObjectMap = denester.getPointerToObjectMap();
        addBasicProperties(pointerToObjectMap, pointerToSchemaMap, co);

        var sourceToTargetWithRelationship = denester.getSourceToTargetWithRelationship();

        // populate external references and use relationship name from schema - if asked
        Map<String, List<ExternalRelative>> sourceToExternalTargetWithRelationship = includeRelationships ? deriveExternalRelationships(co.content, pointerToSchemaMap) : new HashMap<>();

        // use relationship name from schema for internal references
        updateInternalRelationshipNames(sourceToTargetWithRelationship, pointerToSchemaMap);

        // execute the Cypher query as shown in the example comment above
        ImmutablePair<StatementBuilder.OngoingUpdate, Node> updateAndRoot = buildUpdateStatement(co.id, co.type, pointerToObjectMap);

        // now build the graph as if it were a create
        var statement = buildGraph(co.id, updateAndRoot.left, updateAndRoot.right, pointerToObjectMap, pointerToSchemaMap, sourceToTargetWithRelationship, sourceToExternalTargetWithRelationship);

        String cypherQuery = Renderer
                .getRenderer(Configuration.prettyPrinting())
                .render(statement);
        logQuery(cypherQuery, config.verbose);

        Value resultValue;
        try (Session session = driver.session(getSessionConfig())) {
            resultValue = session.writeTransaction(tx -> {
                Result result = tx.run(cypherQuery);
                List<Record> records = result.list();
                Value firstResult = null;
                int i = 0;
                for (Record rec : records) {
                    if (i++ == 0) {
                        firstResult = rec.get(0);
                        if (!config.verbose) break;
                    }
                    System.out.println();
                    System.out.println(rec.get(0).toString());
                }
                return firstResult; // TODO: for some reason, result.single().get(0) returns duplicates of the same Node with cypher-dsl, although Cypher query applied directly on Neo4j returns only one Node.
            });
        }
        return resultValue;
    }

    private static ImmutablePair<StatementBuilder.OngoingUpdate, Node> buildUpdateStatement(String id, String type, Map<String, JsonObject> pointerToObjectMap) {
        JsonObject rootObject = pointerToObjectMap.get("");
        Node rootNode = Cypher.node(type,"CordraObject")
                .named("root")
                .withProperties("_id", Cypher.literalOf(id));

        Node internalNode = Cypher.node("_CordraObject")
                .named("internal");

        Node externalNode = Cypher.node("CordraObject")
                .named("external");

        Relationship internalRelationships = rootNode.relationshipTo(internalNode).unbounded();
        Relationship externalRelationships = rootNode.relationshipTo(externalNode);
        var update = Cypher.merge(rootNode)
                .set(rootNode, Cypher.mapOf(keysAndValues(rootObject)))
                .with(rootNode)
                .optionalMatch(internalRelationships)
                .detachDelete(internalNode)
                .with(rootNode)
                .optionalMatch(externalRelationships)
                .delete(externalRelationships);

        return new ImmutablePair<>(update, rootNode);
    }

    public Value create(CordraObject co, Map<String, JsonElement> pointerToSchemaMap) {
        if (!shouldIndexType(co.type)) return null;

        // denest Json into non-nested objects and relationships
        Json denester = new Json(co.content);
        denester.denest();
        var pointerToObjectMap = denester.getPointerToObjectMap();
        addBasicProperties(pointerToObjectMap, pointerToSchemaMap, co);

        var sourceToTargetWithRelationship = denester.getSourceToTargetWithRelationship();

        // populate external references and use relationship name from schema
        var sourceToExternalTargetWithRelationship = deriveExternalRelationships(co.content, pointerToSchemaMap);

        // use relationship name from schema for internal references
        updateInternalRelationshipNames(sourceToTargetWithRelationship, pointerToSchemaMap);

        ImmutablePair<StatementBuilder.OngoingUpdate, Node> updateAndRoot = buildCreateStatement(pointerToObjectMap);

        var statement = buildGraph(co.id, updateAndRoot.left, updateAndRoot.right, pointerToObjectMap, pointerToSchemaMap, sourceToTargetWithRelationship, sourceToExternalTargetWithRelationship);

        String cypherQuery = Renderer
                .getRenderer(Configuration.prettyPrinting())
                .render(statement);
        logQuery(cypherQuery, config.verbose);

        Value resultValue;
        try (Session session = driver.session(getSessionConfig())) {
            resultValue = session.writeTransaction(tx -> {
                Result result = tx.run(cypherQuery);
                return result.single().get(0);
            });
        }
        return resultValue;
    }

    private static ImmutablePair<StatementBuilder.OngoingUpdate, Node> buildCreateStatement(Map<String, JsonObject> pointerToObjectMap) {
        JsonObject rootObject = pointerToObjectMap.get("");
        Node rootNode = Cypher.node(rootObject.get("_type").getAsString(), "CordraObject")
                .named("root")
                .withProperties(keysAndValues(rootObject));

        // Starting point for building a Cypher statement
        return new ImmutablePair<>(Cypher.merge(rootNode), rootNode);
    }

    private static void addBasicProperties(Map<String, JsonObject> pointerToObjectMap, Map<String, JsonElement> pointerToSchemaMap, CordraObject co) {
        for (Map.Entry<String, JsonObject> entry : pointerToObjectMap.entrySet()) {
            String pointer = entry.getKey();
            JsonObject object = entry.getValue();
            if (pointer.equals("")) {
                object.addProperty("_id", co.id);
                object.addProperty("_type", co.type);
            } else {
                object.addProperty("_id", co.id + ":" + pointer);
                String label = getLabel(null, pointerToSchemaMap.get(pointer));
                if (label != null) {
                    object.addProperty("_type", label);
                }
            }
        }
    }

    private static void updateInternalRelationshipNames(Map<String, List<Json.Relative>> sourceToTargetWithRelationship, Map<String, JsonElement> pointerToSchemaMap) {
        sourceToTargetWithRelationship.forEach((pointer, relatives) -> {
            relatives.forEach((relative) -> {
                String property  = relative.relationship;
                JsonElement subSchemaElement = pointerToSchemaMap.get(pointer + "/" + property);
                relative.relationship = getRelationshipType(property, subSchemaElement);
            });
        });
    }

    private static Map<String, List<ExternalRelative>> deriveExternalRelationships(JsonElement content, Map<String, JsonElement> pointerToSchemaMap) {
        Map<String, List<ExternalRelative>> sourceToExternalTargetWithRelationship = new HashMap<>();
        for (Map.Entry<String, JsonElement> entry : pointerToSchemaMap.entrySet()) {
            String pointer = entry.getKey();
            JsonElement value = JsonUtil.getJsonAtPointer(content, pointer);
            if (value.isJsonPrimitive()) {
                JsonElement subSchemaElement = pointerToSchemaMap.get(pointer);
                if (isExternalReference(subSchemaElement)) {
                    // if the pointer is an array entry, level up for getting property name. E.g., /pets/0 -> /pets
                    if (JsonUtil.getLastSegmentFromJsonPointer(pointer).matches("^0$|^[1-9]\\d*$")) {
                        pointer = JsonUtil.getParentJsonPointer(pointer);
                    }
                    String property = JsonUtil.getLastSegmentFromJsonPointer(pointer);
                    String relationship = getRelationshipType(property, subSchemaElement);
                    String parentPointer = JsonUtil.getParentJsonPointer(pointer);
                    sourceToExternalTargetWithRelationship.computeIfAbsent(parentPointer, k -> new ArrayList<>()).add(new ExternalRelative(relationship, value.getAsString()));
                }
            }
        }
        return sourceToExternalTargetWithRelationship;
    }

    private static ResultStatement buildGraph(
            String id,
            StatementBuilder.OngoingUpdate ongoingUpdate,
            Node rootNode,
            Map<String, JsonObject> pointerToObjectMap,
            Map<String, JsonElement> pointerToSchemaMap,
            Map<String, List<Json.Relative>> sourceToTargetWithRelationship,
            Map<String, List<ExternalRelative>> sourceToExternalTargetWithRelationship) {

        Map<String, Node> pointerToNodeMap = new HashMap<>();
        pointerToNodeMap.put("", rootNode);

        // Iterate through the list of child nodes
        int i = 0;
        for (Map.Entry<String, JsonObject> entry : pointerToObjectMap.entrySet()) {
            String pointer = entry.getKey();
            if (!pointer.equals("")) {
                JsonObject childObject = entry.getValue();
                Node childNode = null;
                String label = getLabel(null, pointerToSchemaMap.get(pointer));
                if (label == null) {
                     childNode = Cypher.node("_CordraObject")
                             .named("child" + i++)
                             .withProperties(keysAndValues(childObject));

                } else {
                    childNode = Cypher.node(label, "_CordraObject")
                            .named("child" + i++)
                            .withProperties(keysAndValues(childObject));
                }
                pointerToNodeMap.put(pointer, childNode);
                ongoingUpdate = ongoingUpdate.merge(childNode);
            }
        }

        // Iterate through the internal relationships
        for (Map.Entry<String, List<Json.Relative>> entry : sourceToTargetWithRelationship.entrySet()) {
            String sourcePointer = entry.getKey();
            Node source = pointerToNodeMap.get(sourcePointer);
            List<Json.Relative> relatives = entry.getValue();
            for (Json.Relative relative: relatives) {
                Node target = pointerToNodeMap.get(relative.targetPointer);
                String relationship = relative.relationship;
                ongoingUpdate = ongoingUpdate.merge(source.relationshipTo(target, relationship));
            }
        }

        // Iterate through the external relationships
        i = 0;
        for (Map.Entry<String, List<ExternalRelative>> entry : sourceToExternalTargetWithRelationship.entrySet()) {
            String sourcePointer = entry.getKey();
            Node source = pointerToNodeMap.get(sourcePointer);
            Node sourceRenamed = source.named("source" + i);
            List<ExternalRelative> relatives = entry.getValue();
            if (relatives.size() > 0) {
                ongoingUpdate = ongoingUpdate
                        .merge(sourceRenamed);
            }
            for (ExternalRelative relative: relatives) {
                Node target = Cypher.node("CordraObject")
                        .named("target" + i)
                        .withProperties("_id", Cypher.literalOf(relative.reference));
                ongoingUpdate = ongoingUpdate
                        .merge(target)
                        .merge(sourceRenamed.relationshipTo(target, relative.relationship));
                i++;
            }
        }

        return ongoingUpdate
                .merge(Cypher
                        .node("CordraObject")
                        .named("finishedRoot")
                        .withProperties("_id", Cypher.literalOf(id)))
                .returning("finishedRoot")
                .build();
    }

    private static class ExternalRelative {
        String relationship;
        String reference;
        public ExternalRelative(String relationship, String reference) {
            this.relationship = relationship;
            this.reference = reference;
        }
    }

    private static boolean isExternalReference(JsonElement subSchemaElement) {
        if (subSchemaElement != null && subSchemaElement.isJsonObject()) {
            JsonObject subSchema = subSchemaElement.getAsJsonObject();
            JsonElement handleReferenceElement = JsonUtil.getJsonAtPointer(subSchema, "/cordra/type/handleReference");
            return handleReferenceElement != null;
        }
        return false;
    }

    private static String getRelationshipType(String defaultName, JsonElement subSchemaElement) {
        if (subSchemaElement != null && subSchemaElement.isJsonObject()) {
            JsonObject subSchema = subSchemaElement.getAsJsonObject();
            JsonElement nameElement = JsonUtil.getJsonAtPointer(subSchema, "/cordra/ext/neo4j/relationshipType");
            return nameElement == null ? defaultName : nameElement.getAsString();
        }
        return defaultName;
    }

    private static String getLabel(String defaultName, JsonElement subSchemaElement) {
        if (subSchemaElement != null && subSchemaElement.isJsonObject()) {
            JsonObject subSchema = subSchemaElement.getAsJsonObject();
            JsonElement nameElement = JsonUtil.getJsonAtPointer(subSchema, "/cordra/ext/neo4j/nodeLabel");
            return nameElement == null ? defaultName : nameElement.getAsString();
        }
        return defaultName;
    }

    private static Object[] keysAndValues(JsonObject jsonObject) {
        List<Object> keysAndValuesList = new ArrayList<>();
        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            String key = entry.getKey();
            Object value = getObjectFromPrimitiveOrArray(entry.getValue());
            keysAndValuesList.add(key);
            keysAndValuesList.add(Cypher.literalOf(value));
        }
        return keysAndValuesList.toArray(new Object[0]);
    }

    private static Object getObjectFromPrimitiveOrArray(JsonElement jsonElement) {
        if (jsonElement.isJsonPrimitive()) {
            return getJsonPrimitiveAsPrimitive(jsonElement.getAsJsonPrimitive());
        } else if (jsonElement.isJsonArray()) {
            ArrayList<Object> list = new ArrayList<>();
            for (JsonElement element : jsonElement.getAsJsonArray()) {
                if (element.isJsonPrimitive()) list.add(getJsonPrimitiveAsPrimitive(element.getAsJsonPrimitive()));
            }
            return list;
        }
        return "";
    }

    private static Object getJsonPrimitiveAsPrimitive(JsonPrimitive j) {
        Gson gson = new Gson();
        Object result = gson.fromJson(j, Object.class);
        return result;
    }

    private static void logQuery(String query, boolean log) {
        if (log) {
            System.out.println();
            System.out.println(query);
        }
    }

    public synchronized void shutdown() {
        if (shutdown) {
            return;
        }
        shutdown = true;
        if (driver  != null) {
            driver.close();
        }
    }

}
