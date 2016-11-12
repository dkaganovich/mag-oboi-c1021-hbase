package edu.gatech.cse6242;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;


public class HBaseKVMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

    private static final byte[] ADJ_LIST_COL_FAMILY = "al".getBytes();
    private static final int NUM_FIELDS = 3;

    private ImmutableBytesWritable hKey = new ImmutableBytesWritable();
    private KeyValue kv;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length != NUM_FIELDS) {
            throw new RuntimeException(String.format("Malformed input file: expected %d fields, got %d", NUM_FIELDS, fields.length));
        }
        hKey.set(itob(Integer.valueOf(fields[1])));
        kv = new KeyValue(hKey.get(),
                ADJ_LIST_COL_FAMILY, itob(Integer.valueOf(fields[0])), itob(Integer.valueOf(fields[2])));
        context.write(hKey, kv);
    }

    private byte[] itob(int i) {
        return ByteBuffer.allocate(4).putInt(i).array();
    }
}
