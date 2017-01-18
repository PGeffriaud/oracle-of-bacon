package com.serli.oracle.of.bacon.repository;

import com.fasterxml.jackson.databind.util.JSONPObject;
import com.google.gson.JsonObject;
import com.mongodb.util.JSON;
import com.serli.oracle.of.bacon.bean.Actor;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class ElasticSearchRepository {

    private final JestClient jestClient;

    public ElasticSearchRepository() {
        jestClient = createClient();

    }

    public static JestClient createClient() {
        JestClient jestClient;
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
                .multiThreaded(true)
                .readTimeout(60000)
                .build());

        jestClient = factory.getObject();
        return jestClient;
    }

    public List<String> getActorsSuggests(String searchQuery) throws IOException {
        JsonObject name = new JsonObject();
        name.addProperty("query", searchQuery);
        name.addProperty("fuzziness", 5);

        JsonObject match = new JsonObject();
        match.add("name", name);

        JsonObject query = new JsonObject();
        query.add("match", match);

        JsonObject body = new JsonObject();
        body.addProperty("from", 0);
        body.addProperty("size", 10);
        body.add("query", query);

        Search search = new Search.Builder(body.toString()).addIndex("bacon").addType("actor").build();
        return jestClient.execute(search)
                         .getHits(Actor.class)
                         .stream()
                         .map(sr -> sr.source.getName())
                         .collect(Collectors.toList());
    }


}
