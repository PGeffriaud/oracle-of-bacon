package com.serli.oracle.of.bacon.api;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import com.serli.oracle.of.bacon.repository.MongoDbRepository;
import com.serli.oracle.of.bacon.repository.Neo4JRepository;
import com.serli.oracle.of.bacon.repository.RedisRepository;
import net.codestory.http.annotations.Get;
import net.codestory.http.payload.Payload;
import org.bson.Document;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class APIEndPoint {
    private final Neo4JRepository neo4JRepository;
    private final ElasticSearchRepository elasticSearchRepository;
    private final RedisRepository redisRepository;
    private final MongoDbRepository mongoDbRepository;

    public APIEndPoint() {
        neo4JRepository = new Neo4JRepository();
        elasticSearchRepository = new ElasticSearchRepository();
        redisRepository = new RedisRepository();
        mongoDbRepository = new MongoDbRepository();
    }

    @Get("bacon-to?actor=:actorName")
    public String getConnectionsToKevinBacon(String actorName) {
        redisRepository.insertSearch(actorName);
        List<?> connectionsToKevinBacon = neo4JRepository.getConnectionsToKevinBacon(actorName);

        return connectionsToKevinBacon.toString();
    }

    @Get("suggest?q=:searchQuery")
    public Payload getActorSuggestion(String searchQuery) {
        try {
            List<String> actorsSuggests = elasticSearchRepository.getActorsSuggests(searchQuery);
            return new Payload(actorsSuggests);
        } catch (IOException e) {
            Random r = new Random();
            return new Payload(String.format("Oh snap ! It crashes. <img src=\"http://placekitten.com/g/%d/%d\" />", r.nextInt(300)+200, r.nextInt(300)+200)).withCode(500);
        }
    }

    @Get("last-searches")
    public List<String> last10Searches() {
        return redisRepository.getLastSearches();
    }

    @Get("actor?name=:actorName")
    public Payload getActorByName(String actorName) {
        return mongoDbRepository.getActorByName(actorName)
                                .map(r -> new Payload(r.toJson()))
                                .orElse(new Payload("").withCode(404));
    }
}
