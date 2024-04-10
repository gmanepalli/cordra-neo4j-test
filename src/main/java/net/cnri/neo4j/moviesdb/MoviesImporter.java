package net.cnri.neo4j.moviesdb;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import net.cnri.cordra.api.CordraClient;
import net.cnri.cordra.api.CordraException;
import net.cnri.cordra.api.CordraObject;
import net.cnri.cordra.api.TokenUsingHttpCordraClient;
import net.cnri.cordra.util.GsonUtility;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;

public class MoviesImporter {

    private static String baseUri;
    private static String username;
    private static String password;

    public static void main(String[] args) throws Exception {
        OptionSet options = parseOptions(args);
        extractOptions(options);
        JsonArray records = loadRecordsJson();
        JsonArray relationships = loadRelationshipsJson();
        try (CordraClient cordra = new TokenUsingHttpCordraClient(baseUri, username, password)) {
//        while (true) {
//            try (SearchResults<String> results = cordra.searchHandles("type:Movie type:Person")) {
//                if (results.size() == 0) break;
//                for (String id : results) {
//                    try {
//                        cordra.delete(id);
//                    } catch (Exception e) {
//                        // ignore
//                    }
//                }
//            }
//        }
            createCordraObjectsFor(records, cordra);
            createRelationshipsFor(relationships, cordra);
        }
    }

    private static OptionSet parseOptions(String[] args) throws IOException {
        OptionParser parser = new OptionParser();
        parser.acceptsAll(Arrays.asList("h", "help")).forHelp();
        parser.acceptsAll(Arrays.asList("b", "base-uri")).withRequiredArg().required();
        parser.acceptsAll(Arrays.asList("u", "username")).withRequiredArg().required();
        parser.acceptsAll(Arrays.asList("p", "password"), "Can be entered as standard input").withRequiredArg();

        OptionSet options;
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            System.out.println("Error parsing options: " + e.getMessage());
            System.out.println("This tool will import sample objects for the Cordra Neo4j test example.");
            parser.printHelpOn(System.out);
            System.exit(1);
            return null;
        }
        if (options.has("h")) {
            System.out.println("This tool will import sample objects for the Cordra Neo4j test example.");
            parser.printHelpOn(System.out);
            System.exit(1);
            return null;
        }
        return options;
    }

    private static void extractOptions(OptionSet options) throws IOException {
        baseUri = (String)options.valueOf("base-uri");
        username = (String)options.valueOf("username");
        password = (String)options.valueOf("password");
        if (password == null) {
            System.out.print("Password: ");
            try (
                InputStreamReader isr = new InputStreamReader(System.in, StandardCharsets.UTF_8);
                BufferedReader reader = new BufferedReader(isr);
            ) {
                password = reader.readLine();
            }
        }
    }

    private static void createRelationshipsFor(JsonArray relationships, CordraClient cordra) throws CordraException {
        for (int i = 0; i < relationships.size(); i++) {
            JsonObject item = relationships.get(i).getAsJsonObject();
            JsonObject r = item.get("r").getAsJsonObject();
            String type = r.get("type").getAsString();
            JsonObject a = item.get("a").getAsJsonObject();
            JsonObject b = item.get("b").getAsJsonObject();
            String from = a.get("identity").getAsString();
            String to = b.get("identity").getAsString();
            String fromId = getId(from);
            String toId   = getId(to);
            CordraObject fromCo = cordra.get(fromId);
            addRelationship(fromCo, type, toId);
            System.out.println(fromId + " - [" + type + "] ->" + toId);
            fromCo = cordra.update(fromCo);
        }
    }

    public static void addRelationship(CordraObject co, String type, String toId) {
        JsonObject content = co.content.getAsJsonObject();
        JsonArray relationships = null;
        if (content.has(type)) {
            relationships = content.get(type).getAsJsonArray();
        } else {
            relationships = new JsonArray();
            content.add(type, relationships);
        }
        relationships.add(toId);
    }

    public static void createCordraObjectsFor(JsonArray records, CordraClient cordra) throws CordraException {
        for (int i = 0; i < records.size(); i++) {
            JsonObject o = records.get(i).getAsJsonObject();
            CordraObject co = fromNeo4jResult(o);
            cordra.create(co);
            System.out.println(GsonUtility.getGson().toJson(co));
        }
    }

    public static String getId(String suffix) {
        return "test/" + suffix;
    }

    public static CordraObject fromNeo4jResult(JsonObject o) {
        JsonObject item = o.get("n").getAsJsonObject();
        CordraObject co = new CordraObject();
        String suffix = item.get("identity").getAsString();
        String id = getId(suffix);
        co.id = id;
        JsonArray labels = item.get("labels").getAsJsonArray();
        co.type = labels.get(0).getAsString();
        JsonObject content = item.get("properties").getAsJsonObject();
        co.content = content;
        return co;
    }

    private static JsonArray loadRelationshipsJson() throws IOException {
        String json = loadResource("/moviesdb/relationships.json");
        return JsonParser.parseString(json).getAsJsonArray();
    }

    private static JsonArray loadRecordsJson() throws IOException {
        String json = loadResource("/moviesdb/records.json");
        return JsonParser.parseString(json).getAsJsonArray();
    }

    private static String loadResource(String filename) throws IOException {
        String result = null;
        try (
            InputStream in = MoviesImporter.class.getResourceAsStream(filename);
            InputStreamReader isr = new InputStreamReader(in, StandardCharsets.UTF_8);
            BufferedReader br = new BufferedReader(isr);
        ) {
            result = br
                    .lines()
                    .collect(Collectors.joining("\n"));
        }
        return result;
    }
}
