package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

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

        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {

                List<Index> currentChunk = new ArrayList<>();

                bufferedReader
                    .lines()
                    .forEach(line -> {

                        Map<String, String> source = new LinkedHashMap<>();
                        source.put("name", line);
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

    private static void insertChunk(JestClient client, List<Index> currentChunk) {
        try {
            client.execute(new Bulk.Builder()
                    .defaultIndex("bacon")
                    .defaultType("actor")
                    .addAction(currentChunk)
                    .build());
            System.out.println(count.addAndGet(currentChunk.size()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        currentChunk.clear();
    }
}
