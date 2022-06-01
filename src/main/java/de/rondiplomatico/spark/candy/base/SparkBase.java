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

public class SparkBase {

    private static SparkSession session;

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

    private static SparkConf readFromFile(String configFile) {
        Properties props = new Properties();
        File in = new File(configFile);
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
        parseEnvironmentVariables(props);
        SparkConf conf = new SparkConf();
        props.forEach((k, v) -> conf.set((String) k, (String) v));
        return conf;
    }

    private static void parseEnvironmentVariables(Properties conf) {
        final Pattern variablePattern = Pattern.compile("\\$\\{(?<variable>[A-Z]+[A-Z0-9_]*)\\}");

        for (Entry<Object, Object> t : conf.entrySet()) {
            String value = (String) t.getValue();
            Matcher m = variablePattern.matcher(value);
            int lastIndex = 0;
            StringBuilder output = new StringBuilder();
            while (m.find()) {
                output.append(value, lastIndex, m.start());
                String envVar = System.getenv(m.group("variable"));
                if (envVar == null) {
                    throw new LearningException("Environment variable " + m.group("variable") + " required for property " + t.getKey()
                                    + ", but is not set.");
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

    private JavaSparkContext jsc;

    public JavaSparkContext getJavaSparkContext() {
        if (jsc == null) {
            jsc = JavaSparkContext.fromSparkContext(getSparkSession().sparkContext());
        }
        return jsc;
    }

    public <T> Dataset<T> toDataset(JavaRDD<T> rdd, Class<T> clazz) {
        return getSparkSession().createDataset(rdd.rdd(), getBeanEncoder(clazz));
    }

    public <T> JavaRDD<T> toJavaRDD(Dataset<Row> dataset, Class<T> clazz) {
        return dataset.as(getBeanEncoder(clazz)).toJavaRDD();
    }

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
