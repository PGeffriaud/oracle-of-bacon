package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.List;

public class RedisRepository {

    private Jedis jedis;
    private static String LAST_SEARCHES = "search:last";
    private static int LAST_SEARCHES_LENGTH = 10;

    public RedisRepository() {
        this.jedis = new Jedis("localhost", 6379);
    }

    /**
     * Get all the last searches from the redis storage
     * @return a list of the last searched string
     */
    public List<String> getLastSearches() {
        return jedis.lrange(LAST_SEARCHES, 0, LAST_SEARCHES_LENGTH - 1);
    }

    /**
     * Insert a new search value in redis storage and trim the list in order to have only the last values.
     * @param search last searched value
     */
    public void insertSearch(String search) {
        jedis.lpush(LAST_SEARCHES, search);
        jedis.ltrim(LAST_SEARCHES, 0, LAST_SEARCHES_LENGTH - 1);
    }
}
