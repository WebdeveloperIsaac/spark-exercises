package de.rondiplomatico.spark.candy.base.data;

import java.io.Serializable;
import java.time.LocalTime;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author wirtzd
 * @since 11.05.2021
 */
@AllArgsConstructor
@Getter
public class Crush implements Serializable {
    private static final long serialVersionUID = 2155658470274598167L;

    private Candy candy;
    private String user;
    private LocalTime time;
}
