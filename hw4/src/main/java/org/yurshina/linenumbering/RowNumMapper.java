package org.yurshina.linenumbering;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Anastasiia_Iurshina
 */
public class RowNumMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

}
