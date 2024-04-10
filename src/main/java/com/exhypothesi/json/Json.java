package com.exhypothesi.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Json {
    public static class Relative {
        public String relationship;
        public String targetPointer;
        public Relative(String relationship, String targetPointer) {
            this.relationship = relationship;
            this.targetPointer = targetPointer;
        }
    }

    private final JsonElement json;
    private final Map<String, JsonObject> pointerToObjectMap;
    private final Map<String, List<Relative>> sourceToTargetWithRelationship;

    public Map<String, JsonObject> getPointerToObjectMap() {
        return pointerToObjectMap;
    }

    public Map<String, List<Relative>> getSourceToTargetWithRelationship() {
        return sourceToTargetWithRelationship;
    }

    public Json(JsonElement json) {
        this.json = json;
        pointerToObjectMap = new HashMap<>();
        sourceToTargetWithRelationship = new HashMap<>();
    }

    public void denest() {
        denestJsonElement(json, "", null, "");
    }

    private void denestJsonElement(JsonElement element, String jsonPointer, String sourceJsonPointer, String relationship) {
        if (element.isJsonObject()) {
            JsonObject obj = element.getAsJsonObject();
            JsonObject target = new JsonObject();

            for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
                if (entry.getValue().isJsonPrimitive()) {
                    target.add(entry.getKey(), entry.getValue());
                } else if (entry.getValue().isJsonArray()) {
                    JsonArray array = denestJsonArray(entry.getValue().getAsJsonArray(), jsonPointer + "/" + entry.getKey(), jsonPointer, entry.getKey());
                    if (array.size() > 0) target.add(entry.getKey(), array);
                } else {
                    denestJsonElement(entry.getValue(), jsonPointer + "/" + entry.getKey(), jsonPointer, entry.getKey());
                }
            }

            pointerToObjectMap.put(jsonPointer, target);
            if (sourceJsonPointer != null) {
                sourceToTargetWithRelationship.computeIfAbsent(sourceJsonPointer, k -> new ArrayList<>()).add(new Relative(relationship, jsonPointer));
            }
        }
    }

    private JsonArray denestJsonArray(JsonArray jsonArray, String jsonPointer, String sourceJsonPointer, String relationship) {
        JsonArray resultArray = new JsonArray();
        int index = 0;
        for (JsonElement element : jsonArray) {
            if (element.isJsonPrimitive()) {
                resultArray.add(element);
            } else if (element.isJsonObject()) {
                denestJsonElement(element, jsonPointer + "/" + index, sourceJsonPointer, relationship);
            }
            index++;
        }
        return resultArray;
    }
}
