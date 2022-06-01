package de.rondiplomatico.spark.candy;

import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;

import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.Utils;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SparkAdvanced extends SparkBase {

    private final JavaSparkContext ctx;

    private Map<String, String> cities = Utils.getHomeCities();

    public static void main(String[] args) {

    }

}
