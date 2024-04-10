package net.cnri.neo4j;

import net.cnri.cordra.api.*;

public class Util {

    public static void ensureNoInboundReferences(CordraObject co, CordraClient cordra) throws CordraException {
        String query = "internal.pointsAt:" + co.id;
        try (SearchResults<String> results = cordra.searchHandles(query)) {
            if (results.size() > 0) {
                throw new BadRequestCordraException("Cannot delete " + co.id + " other objects still point at it.");
            }
        }
    }
}
