package de.rondiplomatico.spark.candy.base;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.rondiplomatico.spark.candy.SparkBasics;

/**
 * This is the base class providing convenience access to the spark environment.
 * It manages creation of the spark session and reads the spark config from the src/main/resources folder.
 * 
 * @since 2022-06-22
 * @author wirtzd
 *
 */
public class SparkBase {

    private static final Logger log = LoggerFactory.getLogger(SparkBase.class);

    /**
     * Pattern used to match variables in spark config files.
     */
    public static final String VAR_PATTERN = "\\$\\{(?<variable>[A-Z]+[A-Z0-9_]*)\\}";

    private static SparkSession session;

    /**
     * Returns the spark session
     * 
     * Lazily evaluated, the session is kept as a singleton.
     * 
     * @return The spark session
     */
    public static SparkSession getSparkSession() {
        if (session == null) {
            Builder b = SparkSession.builder();
            if (System.getenv("SPARK_YARN_STAGING_DIR") == null || !System.getenv("SPARK_YARN_STAGING_DIR").contains("abfs://hdinsight")) {
                b.config(readFromFile("spark.conf"));
            }
            session = b.getOrCreate();
        }
        return session;
    }

    /**
     * Utility method to read the spark config from a local config file (either from resources or local file system)
     * 
     * Also parses any environment variables declared using the variable pattern {@link VAR_PATTERN}.
     * 
     * @param configFile
     * @return A SparkConf object with the parameters from the config file.
     */
    private static SparkConf readFromFile(String configFile) {
        Properties props = new Properties();
        File in = new File(configFile);
        // For relative paths, chech the classpath for according files first.
        if (!in.isAbsolute()) {
            try (InputStream is = ClassLoader.getSystemResourceAsStream(configFile)) {
                if (is == null) {
                    throw new LearningException("Resource file " + configFile + " not found on ClassPath");
                } else {
                    props.load(is);
                }
            } catch (IOException e) {
                throw new LearningException("Failed loading config file " + configFile + " from resources", e);
            }
        }
        // Parse any env vars
        parseEnvironmentVariables(props);
        // Copy the variables to a spark conf object (no direct constructor available)
        SparkConf conf = new SparkConf();
        props.forEach((k, v) -> conf.set((String) k, (String) v));
        return conf;
    }

    /**
     * Reads all entries from the provided Properties and replaces any variable matching the pattern {@link VAR_PATTERN} by corresponding environment variables.
     * 
     * @param conf
     */
    private static void parseEnvironmentVariables(Properties conf) {
        final Pattern variablePattern = Pattern.compile(VAR_PATTERN);

        for (Entry<Object, Object> t : conf.entrySet()) {
            String value = (String) t.getValue();
            Matcher m = variablePattern.matcher(value);
            int lastIndex = 0;
            StringBuilder output = new StringBuilder();
            while (m.find()) {
                output.append(value, lastIndex, m.start());
                String envVar = System.getenv(m.group("variable"));
                if (envVar == null) {
                    log.warn("Environment variable {} required for property {}, but is not set. Variable pattern: {}",
                              m.group("variable"), t.getKey(),
                              variablePattern);
                }
                output.append(envVar);
                lastIndex = m.end();
            }
            if (lastIndex < value.length()) {
                output.append(value, lastIndex, value.length());
            }
            conf.setProperty((String) t.getKey(), output.toString());
        }
    }

    /**
     * The instance-based java spark context.
     * Wraps the scala spark context object and provides access methods for the java interfaces to spark objects.
     */
    private JavaSparkContext jsc;

    /**
     * Returns the java spark context of this class.
     * 
     * Is lazily initiated and kept as a singleton for this instance
     * 
     * @return
     */
    public JavaSparkContext getJavaSparkContext() {
        if (jsc == null) {
            jsc = JavaSparkContext.fromSparkContext(getSparkSession().sparkContext());
        }
        return jsc;
    }

    /**
     * Helper method to convert a spark java RDD to a Spark SQL Dataset.
     * 
     * @param <T>
     *            The generic RDD data type
     * @param rdd
     *            The RDD to convert
     * @param clazz
     *            The class of the generic type to create the bean encoder from
     * @return A spark sql dataset
     */
    public <T> Dataset<T> toDataset(JavaRDD<T> rdd, Class<T> clazz) {
        return getSparkSession().createDataset(rdd.rdd(), getBeanEncoder(clazz));
    }

    /**
     * Helper method to create a java RDD from a Spark SQL Dataset.
     * 
     * @param <T>
     *            The generic RDD data type
     * @param rdd
     *            The dataset to convert
     * @param clazz
     *            The class of the generic type to create the bean encoder from
     * @return A spark typed java RDD
     */
    public <T> JavaRDD<T> toJavaRDD(Dataset<Row> dataset, Class<T> clazz) {
        return dataset.as(getBeanEncoder(clazz)).toJavaRDD();
    }

    /**
     * Encoders are the main tool to map java objects to spark sql struct types and vice versa.
     * 
     * This takes a lot of work from programmers who want to work with POJO's in their code and "just want to store them" in a e.g. parquet file
     * 
     * However, automatic conversion for complex nested POJOs is not always working and for some objects the bean encoder will fail, as we will see in the
     * exercises.
     * 
     * @param <T>
     * @param clazz
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T> Encoder<T> getBeanEncoder(Class<T> clazz) {
        if (String.class.equals(clazz)) {
            return (Encoder<T>) Encoders.STRING();
        } else if (Integer.class.equals(clazz)) {
            return (Encoder<T>) Encoders.INT();
        } else if (Double.class.equals(clazz)) {
            return (Encoder<T>) Encoders.DOUBLE();
        } else if (Float.class.equals(clazz)) {
            return (Encoder<T>) Encoders.FLOAT();
        } else if (Long.class.equals(clazz)) {
            return (Encoder<T>) Encoders.LONG();
        } else if (Boolean.class.equals(clazz)) {
            return (Encoder<T>) Encoders.BOOLEAN();
        } else if (Byte.class.equals(clazz)) {
            return (Encoder<T>) Encoders.BYTE();
        } else if (byte[].class.equals(clazz)) {
            return (Encoder<T>) Encoders.BINARY();
        }
        return Encoders.bean(clazz);
    }

}
