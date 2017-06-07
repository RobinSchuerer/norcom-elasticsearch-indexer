package de.robinschuerer.json;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import javax.annotation.Nonnull;

import org.springframework.core.io.FileSystemResource;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonReader {

    @Nonnull
    public static ObjectNode read(@Nonnull final String pathToJson) {
        return read(new FileSystemResource(pathToJson).getFile());
    }

    @Nonnull
    public static ObjectNode read(@Nonnull final File file) {
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonFactory factory = new JsonFactory(objectMapper);
        try {
            final JsonParser parser = factory.createParser(file);
            parser.nextToken();

            return parser.readValueAsTree();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}

