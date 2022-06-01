package de.rondiplomatico.spark.candy.base;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

public class SparkBase {

    private static SparkSession session;

    public static SparkSession getSparkSession() {
        if (session == null) {
            Builder b = SparkSession.builder();
            if (!SparkSession.getActiveSession().isDefined()) {
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
                    throw new RuntimeException("Resource file " + configFile + " not found on ClassPath");
                } else {
                    props.load(is);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed loading config file " + configFile + " from resources", e);
            }
        }
        SparkConf conf = new SparkConf();
        props.forEach((k, v) -> conf.set((String) k, (String) v));
        return conf;
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

    @SuppressWarnings("unchecked")
    private <T> Encoder<T> getBeanEncoder(Class<T> clazz) {
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
