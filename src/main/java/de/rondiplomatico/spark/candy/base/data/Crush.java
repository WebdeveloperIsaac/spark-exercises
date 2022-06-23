package de.rondiplomatico.spark.candy.base.data;

import java.io.Serializable;
import java.time.LocalTime;

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
