import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Anastasiia_Iurshina
 */
public class IntDoublePair implements WritableComparable {

    private long sum;
    private double avg;

    public IntDoublePair() {
    }

    public IntDoublePair(final long sum, final double avg) {
        this.sum = sum;
        this.avg = avg;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(sum);
        out.writeDouble(avg);
    }

    public void readFields(DataInput in) throws IOException {
        sum = in.readLong();
        avg = in.readDouble();
    }

    @Override
    public int hashCode() {
        return (int) (sum * 157 + avg);
    }
    @Override
    public boolean equals(Object right) {
        if (right instanceof IntDoublePair) {
            IntDoublePair r = (IntDoublePair) right;
            return r.sum == sum && r.avg == avg;
        } else {
            return false;
        }
    }

    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(IntDoublePair.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static {
        WritableComparator.define(IntDoublePair.class, new Comparator());
    }

    public int compareTo(Object that) {
        if (that == null) {
            return -1;
        }

        int sumRes = Long.compare(this.sum, ((IntDoublePair) that).sum);
        if (sumRes != 0) {
            return sumRes;
        }

        return Double.compare(this.avg, ((IntDoublePair) that).avg);
    }

    @Override
    public String toString() {
        return avg + "," + sum;
    }
}
