package com.mongodb.yarn;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.util.JSON;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class YarnApp {
    private static final Log LOG = LogFactory.getLog(YarnApp.class);
    private static final String BASE_PATH = System.getProperty("mongo.export.path", "/mongodb/");
    private boolean wasRun = false;
    FileSystem fileSystem;
    private final MongoClientURI uri;

    public YarnApp() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        fileSystem = FileSystem.get(conf);
        uri = new MongoClientURI(System.getProperty(MongoConfigUtil.INPUT_URI));
    }

    public void run() throws IOException {
        MongoClient client = new MongoClient(uri);
        DBCollection collection = client.getDB(uri.getDatabase()).getCollection(uri.getCollection());
        
        String query = System.getProperty(MongoConfigUtil.INPUT_QUERY);
        DBCursor cursor = query == null ? collection.find() : collection.find((DBObject) JSON.parse(query));
        DefaultDBEncoder encoder = new DefaultDBEncoder();
        
        fileSystem.delete(new Path(BASE_PATH + "/" + collection), true);
        FSDataOutputStream out = fileSystem.create(new Path(BASE_PATH + "/" + collection));
        
        while (cursor.hasNext()) {
            out.write(encoder.encode(cursor.next()));
        }
        
        out.close();
    }

    public static void main(String[] args) throws IOException {
        new YarnApp().run();
    }
}
