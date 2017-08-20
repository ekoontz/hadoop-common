package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BuggyWordCount {

  public static class TokenizerMapper 
       extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    int failCount = 0;
    int attemptIndex = 0;
    long inputLength = 0;
    long thisStart = 0;
    
    protected void setup(Context context
        ) throws IOException, InterruptedException {
      failCount = context.getConfiguration().getInt("hao.failCount", 0);
      attemptIndex = context.getTaskAttemptID().getId();
      inputLength = context.getInputSplit().getLength();
      if (context.getInputSplit() instanceof FileSplit) {
        FileSplit fs = (FileSplit)context.getInputSplit();
        thisStart = fs.getStart();
        System.out.println("MAP attemptIndex = " + attemptIndex + " failCount = " + failCount);
      } else {
        System.out.println("NOT FILESPLIT MAP attemptIndex = " + attemptIndex + " failCount = " + failCount);
      }
    }
      
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      long currentPos = key.get() - thisStart;
      if (currentPos * (failCount + 1) - inputLength * (attemptIndex + 1) > 0) {
        throw new IOException("HAO FAIL: " + currentPos + "/" + inputLength);
      }
//      System.out.println("HAO FAIL: " + currentPos + "/" + inputLength);
      
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
