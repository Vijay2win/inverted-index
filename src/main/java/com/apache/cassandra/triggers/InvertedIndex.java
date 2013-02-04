package com.apache.cassandra.triggers;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.SnitchProperties;
import org.apache.cassandra.triggers.ITrigger;

public class InvertedIndex implements ITrigger {
    private Properties properties = loadProperties();

    public Collection<RowMutation> augment(ByteBuffer key, ColumnFamily update) {
        List<RowMutation> mutations = new ArrayList<RowMutation>();
        for (ByteBuffer name : update.getColumnNames()) {
            RowMutation mutation = new RowMutation(properties.getProperty("keyspace"), update.getColumn(name).value());
            ColumnFamily columnFamily = ColumnFamily.create(properties.getProperty("keyspace"),
                    properties.getProperty("columnfamily"));
            columnFamily.addColumn(new Column(name, key, System.currentTimeMillis()));
            mutation.add(columnFamily);
            mutations.add(mutation);
        }
        return mutations;
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        InputStream stream = SnitchProperties.class.getClassLoader().getResourceAsStream("InvertedIndex.properties");
        try {
            properties.load(stream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            FileUtils.closeQuietly(stream);
        }
        return properties;
    }
}
