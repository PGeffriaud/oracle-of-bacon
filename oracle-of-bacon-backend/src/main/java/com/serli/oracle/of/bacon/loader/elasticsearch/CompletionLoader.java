package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.google.gson.JsonObject;
import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.PutMapping;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        String inputFilePath = args[0];
        JestClient client = ElasticSearchRepository.createClient();

        client.execute(new CreateIndex.Builder("bacon").build());
        createMapping(client);

        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {

            // Parse CSV file into reading object
            CSVParser csvParser = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(bufferedReader);

            List<Index> currentChunk = new ArrayList<>();

            // Insert records in ElasticSearch by chunk of 50000 elements
            csvParser
                    .getRecords()
                    .forEach(record -> {
                        Map<String, String> source = new LinkedHashMap<>();
                        source.put("name", record.get("name:ID"));
                        currentChunk.add(new Index.Builder(source).build());

                        if(currentChunk.size() == 50000) {
                            insertChunk(client, currentChunk);
                        }
                    });
                if(!currentChunk.isEmpty()) {
                    insertChunk(client, currentChunk);
                }
        }

        System.out.println("Inserted total of " + count.get() + " actors");
    }

    /**
     * Create ElasticSearch mapping in order to user suggestions
     * @param client
     */
    private static void createMapping(JestClient client) throws IOException {
        JsonObject name = new JsonObject();
        name.addProperty("type", "text");

        JsonObject properties = new JsonObject();
        properties.add("name", name);

        JsonObject actor = new JsonObject();
        actor.add("properties", properties);

        PutMapping putMapping = new PutMapping.Builder("bacon", "actor", actor.toString()).build();
        System.err.println(client.execute(putMapping).getErrorMessage());
    }

    /**
     * Insert a collection of indexes in ElasticSearch
     * @param client ElasticSearch client
     * @param currentChunk collection of indexes
     */
    private static void insertChunk(JestClient client, List<Index> currentChunk) {
        try {
            client.execute(new Bulk.Builder()
                    .defaultIndex("bacon")
                    .defaultType("actor")
                    .addAction(currentChunk)
                    .build());
            System.out.println(count.addAndGet(currentChunk.size()) + " insertions");
        } catch (IOException e) {
            e.printStackTrace();
        }
        currentChunk.clear();
    }
}
