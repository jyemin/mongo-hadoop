package com.mongodb.yarn;

import com.beust.jcommander.IStringConverter;
import com.mongodb.MongoClientURI;

public class MongoURIConverter implements IStringConverter<MongoClientURI> {
    @Override
    public MongoClientURI convert(final String value) {
        return new MongoClientURI(value);
    }
}
