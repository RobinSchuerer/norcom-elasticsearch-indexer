package de.robinschuerer.base.argument;

import java.io.File;

import javax.annotation.Nonnull;

import org.springframework.stereotype.Service;

@Service
public class ArgumentParser {

    @Nonnull
    public IndexerConfiguration parse(final String[] args) {
        if (args.length != 2) {
            return IndexerConfiguration.invalid("Please specify path to data json: -input PATH");
        }

        final String type = args[0];
        if (!"-input".equals(type)) {
            return IndexerConfiguration.invalid(String.format("Unknown argument: %s", type));
        }

        final String path = args[1];
        final File file = new File(path);
        if (!file.exists() || file.isDirectory()) {
            return IndexerConfiguration.invalid(String.format("No data file found: %s", path));

        }

        return IndexerConfiguration
            .newBuilder()
            .withDataFile(file)
            .build();
    }

}
