package com.mongodb.yarn;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class BsonExportClientTest {

    public static final String DATABASE = "mongo_hadoop";
    public static final String COLLECTION = "messages";
    public static final String[][] MATRIX
        = new String[][]{{"--mongoUri", "mongodb://localhost:27017/" + DATABASE + "." + COLLECTION},
                         {"-db", DATABASE, "-c", COLLECTION},
                         {"-db", DATABASE, "--collection", COLLECTION},
                         {"--database", DATABASE, "-c", COLLECTION},
                         {"--database", DATABASE, "--collection", COLLECTION},
                         {"-db", DATABASE, "-c", COLLECTION, "--query", "{mailbox \\: \"bass-e\"}"},
                         {"-db", DATABASE, "-c", COLLECTION, "-q", "{mailbox \\: \"bass-e\"}"},};

    @Test
    public void testRun() throws Exception {
        for (String[] args : MATRIX) {
            BsonExportClient client = new BsonExportClient(args);
            FileSystem fileSystem = FileSystem.get(client.getConf());
            Path outputPath = new Path("/mongodb/" + COLLECTION);
            fileSystem.delete(outputPath, true);
            Assert.assertTrue("Should run successfully with " + Arrays.toString(args), client.run());
            FileStatus fileStatus = fileSystem.getFileStatus(outputPath);
            System.out.println("fileStatus = " + fileStatus);
            Assert.assertTrue("Should output the file " + outputPath, fileSystem.exists(outputPath));

        }
    }
}
