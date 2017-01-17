package com.serli.oracle.of.bacon.repository;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.Optional;

public class MongoDbRepository {

    private final MongoClient mongoClient;

    public MongoDbRepository() {
        mongoClient = new MongoClient("localhost", 27017);
    }

    public Optional<Document> getActorByName(String name) {
        return Optional.ofNullable(
                mongoClient .getDatabase("bacon")
                            .getCollection("actors")
                            .find(Filters.eq("name:ID", name))
                            .first());
    }
}
