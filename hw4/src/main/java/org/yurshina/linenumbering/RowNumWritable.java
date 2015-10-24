package org.yurshina.linenumbering;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Anastasiia_Iurshina
 */
public class RowNumWritable implements Writable {

    //used if contains offset info
    private long offset;
    private int partition;

    //used if it's a row from file
    private Text line;

    public void setLine(final Text line) {
        this.line = line;
    }

    public void setOffset(final long offset) {
        this.offset = offset;
    }

    public void setPartition(final int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public int getPartition() {
        return partition;
    }

    public Text getLine() {
        return line;
    }

    public void write(final DataOutput out) throws IOException {
        if (line == null) {
            out.writeInt(partition);
            out.writeLong(offset);
            line = new Text("");
        }

        line.write(out);
    }

    public void readFields(final DataInput in) throws IOException {
        if (line == null) {
            line = new Text();
        }

        if (line.getLength() == 0) {
            partition = in.readInt();
            offset = in.readLong();
        }
        line.readFields(in);
    }
}
