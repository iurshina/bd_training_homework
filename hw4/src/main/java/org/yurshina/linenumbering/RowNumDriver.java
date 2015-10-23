package org.yurshina.linenumbering;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Anastasiia_Iurshina
 */
public class RowNumDriver extends Configured implements Tool {

    public static void main(final String[] args) throws Exception {
        ToolRunner.run(new RowNumDriver(), args);
        System.exit(1);
    }

    public int run(String[] args) throws Exception {
        return 0;
    }
}
