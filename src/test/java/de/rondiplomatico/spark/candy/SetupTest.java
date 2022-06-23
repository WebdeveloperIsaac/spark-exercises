package de.rondiplomatico.spark.candy;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import de.rondiplomatico.spark.candy.base.SparkBase;

/**
 * Simple quick test to see if you local spark setup is up & running.
 * 
 * @author wirtzd
 *
 */
public class SetupTest extends SparkBase {

    @Test
    public void testFileIO() {
        int n = 100;
        Random r = new Random();
        List<Integer> data = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            data.add(r.nextInt());
        }

        toDataset(getJavaSparkContext().parallelize(data), Integer.class).write()
                                                                         .parquet("testout");

        List<Integer> res = new ArrayList<>(toJavaRDD(getSparkSession().read()
                                                                       .parquet("testout"),
                                                      Integer.class).collect());
        Collections.sort(data);
        Collections.sort(res);
        assertEquals("Written data could not be read and is equal", data, res);
    }

    @After
    public void cleanUp() throws IOException {
        FileUtils.deleteDirectory(new File("testout"));
    }

}
