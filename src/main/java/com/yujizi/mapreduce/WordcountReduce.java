package com.yujizi.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: ychw
 * @Description:
 * @Date: 2020/11/29 13:29
 */
public class WordcountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v=new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;

        for (IntWritable value : values) {

            sum+=value.get();

        }
        v.set(sum);
        context.write(key,v);

    }
}
