package net.cnri.neo4j;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import net.cnri.cordra.*;
import net.cnri.cordra.api.CordraClient;
import net.cnri.cordra.api.CordraException;
import net.cnri.cordra.api.CordraObject;
import net.cnri.cordra.util.GsonUtility;
import net.cnri.cordra.util.JsonUtil;

import java.util.Map;

@CordraServiceHooks
public class ServiceLevelHooks implements CordraTypeInterface {

    private static CordraClient cordra = CordraHooksSupportProvider.get().getCordraClient();

    @Override
    public void afterCreateOrUpdate(CordraObject obj, HooksContext context) throws CordraException {
        Map<String, JsonElement> pointerToSchemaMap = context.pointerToSchemaMap;
        Neo4jCordraObjectIndexer neo4j = Neo4jCordraObjectIndexer.getInstance();
        boolean includeRelationships = true;
        try {
            if (context.isNew) {
                neo4j.create(obj, pointerToSchemaMap); //TODO boolean includeRelationships
            } else {
                neo4j.update(obj, pointerToSchemaMap, includeRelationships);
            }
        } catch (Exception e) {
            //no-op
        }
    }

    @Override
    public void beforeDelete(CordraObject co, HooksContext context) throws CordraException {
        Util.ensureNoInboundReferences(co, cordra);
    }

    @Override
    public void afterDelete(CordraObject obj, HooksContext context) throws CordraException {
        Neo4jCordraObjectIndexer neo4j = Neo4jCordraObjectIndexer.getInstance();
        try {
            neo4j.delete(obj);
        } catch (Exception e) {
            //no-op
        }
    }

    @CordraMethod
    public static JsonElement reloadNeo4jConfig(@SuppressWarnings("unused") HooksContext context) throws Exception {
        Neo4jConfig config = Neo4jCordraObjectIndexer.getInstance().loadConfigFromDesign();
        JsonElement configJson = GsonUtility.getGson().toJsonTree(config);
        return configJson;
    }

    @CordraMethod
    public static JsonElement deleteAllInNeo4j(@SuppressWarnings("unused") HooksContext context) throws Exception {
        Neo4jCordraObjectIndexer.getInstance().deleteAll();
        JsonElement result = new JsonObject();
        return result;
    }

    @CordraMethod
    public static JsonElement deleteInNeo4j(HooksContext context) throws Exception {
        JsonObject attributes = context.attributes;
        if (!attributes.has("id")) {
            throw new Exception("Missing id attribute");
        }
        String id = attributes.get("id").getAsString();
        CordraObject co = cordra.get(id);
        Neo4jCordraObjectIndexer.getInstance().delete(co);
        JsonElement result = new JsonObject();
        return result;
    }

    @CordraMethod
    public static JsonElement searchNeo4j(HooksContext context) throws Exception {
        JsonObject attributes = context.attributes;
        if (!attributes.has("query")) {
            throw new Exception("Missing query attribute");
        }
        String query = attributes.get("query").getAsString();
        return Neo4jCordraObjectIndexer.getInstance().search(query);
    }

    @CordraMethod
    public static JsonElement reindexAllInNeo4j(HooksContext context) throws Exception {
        JsonObject attributes = context.attributes;
        boolean includeRelationships = JsonUtil.getBooleanProperty(attributes, "includeRelationships", true);
        return Neo4jCordraObjectIndexer.getInstance().reindexAll(includeRelationships);
    }

    @CordraMethod
    public static JsonElement reindexAllInNeo4jTwoPass(@SuppressWarnings("unused") HooksContext context) throws Exception {
        Neo4jCordraObjectIndexer.getInstance().reindexAll(false);
        return Neo4jCordraObjectIndexer.getInstance().reindexAll(true);
    }

    @CordraMethod
    public static JsonElement reindexQueryResultsInNeo4j(HooksContext context) throws Exception {
        JsonObject attributes = context.attributes;
        if (!attributes.has("query")) {
            throw new Exception("Missing query attribute");
        }
        String query = attributes.get("query").getAsString();
        boolean includeRelationships = JsonUtil.getBooleanProperty(attributes, "includeRelationships", true);
        return Neo4jCordraObjectIndexer.getInstance().reindexQueryResults(query, includeRelationships);
    }

    @CordraMethod
    public static JsonElement reindexQueryResultsInNeo4jTwoPass(HooksContext context) throws Exception {
        JsonObject attributes = context.attributes;
        if (!attributes.has("query")) {
            throw new Exception("Missing query attribute");
        }
        String query = attributes.get("query").getAsString();
        Neo4jCordraObjectIndexer.getInstance().reindexQueryResults(query, false);
        return Neo4jCordraObjectIndexer.getInstance().reindexQueryResults(query, true);
    }

    @CordraMethod
    public static JsonElement reindexOneInNeo4j(HooksContext context) throws Exception {
        JsonObject attributes = context.attributes;
        if (!attributes.has("id")) {
            throw new Exception("Missing id attribute");
        }
        String id = attributes.get("id").getAsString();
        boolean includeRelationships = JsonUtil.getBooleanProperty(attributes, "includeRelationships", true);
        return Neo4jCordraObjectIndexer.getInstance().reindexId(id, includeRelationships);
    }
}
