package com.serli.oracle.of.bacon.repository;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

public class Neo4JRepository {

    public static final String ACTOR = "Actor";
    public static final String MOVIE = "Movie";
    private final Driver driver;

    public Neo4JRepository() {
        driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "admin"));
    }

    public List<?> getConnectionsToKevinBacon(String actorName) {
        Session session = driver.session();
        List<GraphItem> graphItemList = new ArrayList<>();

        String query = "MATCH p=shortestPath((bacon:Actor {name:'Bacon, Kevin (I)'})-[*]-(a:Actor {name:{actorName}})) RETURN p";
        StatementResult result = session.run(query, Values.parameters("actorName", actorName));

        for (Record record : result.list()) {
            for (Node n : record.get("p").asPath().nodes()) {
                List<String> types = iterableToList(n.labels());
                if(types.contains(ACTOR)){
                    graphItemList.add(new GraphNode(n.id(), n.get("name").toString(), ACTOR));
                } else if(types.contains(MOVIE)){
                    graphItemList.add(new GraphNode(n.id(), n.get("title").toString(), MOVIE));
                }
            }
            for (Relationship r : record.get("p").asPath().relationships())
                graphItemList.add(new GraphEdge(r.id(), r.startNodeId(), r.endNodeId(), r.type()));
        }
        return graphItemList;
    }

    private <T> List<T> iterableToList(Iterable<T> iterable) {
        List<T> list = new ArrayList<T>();
        for (T t : iterable) {
            list.add(t);
        }
        return list;
    }

    private static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }

        @Override
        public String toString() {
            return String.format("{\"data\":" +
                                    "{\"id\":%s, \"value\":%s, \"type\":\"%s\"}}", id, value, type);
        }
    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("{\"data\":" +
                                    "{\"id\":%s, \"source\":%s, \"target\":%s, \"value\":\"%s\"}}", id, source, target, value);
        }
    }
}
