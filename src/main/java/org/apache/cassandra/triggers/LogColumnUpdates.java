package org.apache.cassandra.triggers;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogColumnUpdates implements ITrigger {
    private static final Logger logger = LoggerFactory.getLogger(InvertedIndex.class);
    private Properties properties = loadProperties();
    String column_family = properties.getProperty("uniq_columnfamily");

    public Collection<RowMutation> augment(ByteBuffer key, ColumnFamily update) {
        List<RowMutation> mutations = new ArrayList<RowMutation>();
        RowMutation mutation = new RowMutation(properties.getProperty("keyspace"), key);
        // add a column counts.
        for (ByteBuffer name : update.getColumnNames())
            mutation.add(column_family, UUIDType.instance.decompose(UUID.randomUUID()), name, System.currentTimeMillis());
        mutations.add(mutation);
        return mutations;
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        InputStream stream = InvertedIndex.class.getClassLoader().getResourceAsStream("InvertedIndex.properties");
        try {
            properties.load(stream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            FileUtils.closeQuietly(stream);
        }
        logger.info("loaded property file, InvertedIndex.properties");
        return properties;
    }
}
