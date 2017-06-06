package de.robinschuerer.base.indexservice;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

@Service
public class ElasticSearchIndexService {

    private final static String INDEX_NAME = "enron";

    private final static String URL = "http://localhost:9200/" + INDEX_NAME+ "/data/";

    private final RestTemplate restTemplate = new RestTemplate();

    public void index(@Nonnull final File dataFile) throws IOException {

        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonFactory factory = new JsonFactory(objectMapper);
        final JsonParser parser = factory.createParser(dataFile);

        while (true) {
            final JsonToken token = parser.nextToken();
            if (token == null) {
                break;
            }

            final TreeNode tree = parser.readValueAsTree();

            final TextNode idNode = (TextNode) tree.get("_id").get("$oid");

            final ObjectNode normalized = (ObjectNode) tree;
            normalized.remove("_id");
            normalized.set("backup_id", idNode);

            final HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            final HttpEntity<String> request = new HttpEntity<>(normalized.toString(), headers);

            try {
                restTemplate.put(URL + UUID.randomUUID().toString(), request);
            } catch (HttpClientErrorException e) {
                System.out.println(e.getResponseBodyAsString());

                throw new RuntimeException(e);
            }

        }
    }
}
