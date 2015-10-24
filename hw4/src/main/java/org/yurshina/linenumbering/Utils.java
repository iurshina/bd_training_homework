package org.yurshina.linenumbering;

/**
 * @author Anastasiia_Iurshina
 */
public class Utils {

    public static final String PARTITIONS_INFO = "PARTITIONS";

    public static int hashByFileName(final String fileName, final int numPartitions){
        int fileIndex = Integer.valueOf(fileName.substring(fileName.length() - 6, fileName.length() - 4));

        return (Integer.valueOf(fileIndex).hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
