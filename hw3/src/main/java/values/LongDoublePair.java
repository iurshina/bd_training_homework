package values;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom writable type.
 *
 * @author Anastasiia_Iurshina
 */
public class LongDoublePair implements WritableComparable {

    private long longValue;
    private double doubleValue;

    public LongDoublePair() {
    }

    public LongDoublePair(final long longValue, final double doubleValue) {
        this.longValue = longValue;
        this.doubleValue = doubleValue;
    }

    public long getLongValue() {
        return longValue;
    }

    public double getDoubleValue() {
        return doubleValue;
    }

    public void write(final DataOutput out) throws IOException {
        out.writeLong(longValue);
        out.writeDouble(doubleValue);
    }

    public void readFields(final DataInput in) throws IOException {
        longValue = in.readLong();
        doubleValue = in.readDouble();
    }

    @Override
    public int hashCode() {
        return 31 * Long.valueOf(longValue).hashCode() + Long.valueOf(Double.doubleToLongBits(doubleValue)).hashCode();
    }

    @Override
    public boolean equals(final Object right) {
        if (right instanceof LongDoublePair) {
            LongDoublePair r = (LongDoublePair) right;
            return r.longValue == longValue && r.doubleValue == doubleValue;
        } else {
            return false;
        }
    }

    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(LongDoublePair.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static {
        WritableComparator.define(LongDoublePair.class, new Comparator());
    }

    public int compareTo(final Object that) {
        if (that == null) {
            return -1;
        }

        int sumRes = Long.compare(this.longValue, ((LongDoublePair) that).longValue);
        if (sumRes != 0) {
            return sumRes;
        }

        return Double.compare(this.doubleValue, ((LongDoublePair) that).doubleValue);
    }

    @Override
    public String toString() {
        return doubleValue + "," + longValue;
    }
}
