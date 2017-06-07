package de.robinschuerer.base.indexservice;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import de.robinschuerer.json.JsonReader;
import de.robinschuerer.json.JsonStreamReader;

@Service
public class ElasticSearchIndexService {

    private final static Logger LOGGER = LoggerFactory.getLogger(ElasticSearchIndexService.class);

    private final static String INDEX_NAME = "enron";

    private final static String URL = "http://localhost:9200/" + INDEX_NAME + "/data";

    private final static String INDEX_URL = "http://localhost:9200/" + INDEX_NAME;

    @Autowired
    private RestTemplate restTemplate;

    public void index(@Nonnull final File dataFile) throws IOException {
        createMapping();

        final AtomicInteger count = new AtomicInteger(0);
        JsonStreamReader.readJsonStream(dataFile,
            (node -> {
                final TextNode idNode = (TextNode) node.get("_id").get("$oid");
                node.remove("_id");
                node.set("backup_id", idNode);

                final HttpEntity<ObjectNode> request = getHttpEntity(node);
                try {
                    this.restTemplate.put(URL + '/' + idNode.textValue(), request);
                    LOGGER.info("{}", count.incrementAndGet());
                } catch (HttpClientErrorException e) {
                    LOGGER.error(e.getResponseBodyAsString(), e);
                    throw new RuntimeException(e);
                }
            })
        );
    }

    private void createMapping() throws IOException {
        final ClassPathResource mappingFile = new ClassPathResource("mapping.json");
        final ObjectNode mapping = JsonReader.read(mappingFile.getFile());

        try {
            final HttpEntity<ObjectNode> request = getHttpEntity(mapping);

            this.restTemplate.put(INDEX_URL, request);
        } catch (HttpClientErrorException e) {
            LOGGER.error(e.getResponseBodyAsString(), e);

            throw new RuntimeException(e);
        }

    }

    private static <T> HttpEntity<T> getHttpEntity(final T mapping) {
        final HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        return new HttpEntity<>(mapping, headers);
    }
}
