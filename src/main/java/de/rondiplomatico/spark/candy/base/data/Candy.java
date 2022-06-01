package de.rondiplomatico.spark.candy.base.data;

import java.io.Serializable;

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
public class Candy implements Serializable, Comparable<Candy> {
    private static final long serialVersionUID = -502567585290162061L;

    private Color color;
    private Deco deco;

    @Override
    public String toString() {
        return color + "/" + deco;
    }

    @Override
    public int compareTo(final Candy candy) {
        int res = color.compareTo(candy.color);
        return res == 0 ? deco.compareTo(candy.deco) : res;
    }
}
