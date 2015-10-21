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
public class LongPair implements WritableComparable {

    private long first = 0;
    private long second = 0;

    public LongPair() {
    }

    public LongPair(final long first, final long second) {
        this.first = first;
        this.second = second;
    }

    public long getFirst() {
        return first;
    }

    public long getSecond() {
        return second;
    }

    public void readFields(DataInput in) throws IOException {
        first = in.readLong();
        second = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(first);
        out.writeLong(second);
    }

    public int hashCode() {
        return 31 * Long.valueOf(first).hashCode() + Long.valueOf(second).hashCode();
    }

    public boolean equals(Object right) {
        if (right instanceof LongPair) {
            LongPair r = (LongPair) right;
            return r.first == first && r.second == second;
        } else {
            return false;
        }
    }

    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(LongPair.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static {
        WritableComparator.define(LongPair.class, new Comparator());
    }

    public int compareTo(Object that) {
        if (that == null) {
            return -1;
        }

        if (first != ((LongPair) that).first) {
            return first < ((LongPair) that).first ? -1 : 1;
        } else if (second != ((LongPair) that).second) {
            return second < ((LongPair) that).second ? -1 : 1;
        } else {
            return 0;
        }
    }
}
