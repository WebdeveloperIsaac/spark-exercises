package de.rondiplomatico.spark.candy.base.data;

import java.io.Serializable;
import java.time.LocalTime;
import java.time.temporal.ChronoField;

import de.rondiplomatico.spark.candy.base.Utils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author wirtzd
 * @since 11.05.2021
 */
 @Data
 @NoArgsConstructor
 @AllArgsConstructor
 public class Crush implements Serializable {
 private static final long serialVersionUID = 2155658470274598167L;

 private Candy candy;
 private String user;
 private LocalTime time;

 }

//@Data
//@NoArgsConstructor
//@AllArgsConstructor
//public class Crush implements Serializable {
//    private static final long serialVersionUID = 2155658470274598167L;
//
//    private Candy candy;
//    private String user;
//    private long time;
//
//    public LocalTime asLocalTime() {
//        return LocalTime.ofNanoOfDay(time);
//    }
//
//    public void setLocalTime(LocalTime value) {
//        time = value.getLong(ChronoField.NANO_OF_DAY);
//    }
//}
