package org.yurshina;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

/**
 * @author Anastasiia_Iurshina
 */
@UDFType(deterministic = false)
public class Rank extends UDF {

    private int counter;
    private String lastKey;

    public int evaluate(final String key) {
        if (!key.equalsIgnoreCase(this.lastKey)) {
            this.counter = 0;
            this.lastKey = key;
        }
        return this.counter++;
    }
}
