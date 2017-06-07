package de.robinschuerer.json;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.springframework.core.io.FileSystemResource;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonStreamReader {

    @Nonnull
    public static void readJsonStream(
        @Nonnull final String pathToJson,
        @Nonnull final Consumer<ObjectNode> consumer) {

        readJsonStream(new FileSystemResource(pathToJson).getFile(), consumer);
    }

    @Nonnull
    public static void readJsonStream(
        @Nonnull final File file,
        @Nonnull final Consumer<ObjectNode> consumer) {

        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonFactory factory = new JsonFactory(objectMapper);
        try {
            final JsonParser parser = factory.createParser(file);

            while (true) {
                final JsonToken token = parser.nextToken();
                if (token == null) {
                    break;
                }

                try {
                    consumer.accept(parser.readValueAsTree());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}

