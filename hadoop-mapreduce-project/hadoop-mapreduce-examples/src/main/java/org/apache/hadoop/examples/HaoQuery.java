package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.util.GenericOptionsParser;

public class HaoQuery {

  public static class FieldParser
       extends Mapper<LongWritable, Text, Text, DoubleWritable>{
    private static final Log LOG = LogFactory.getLog(JobImpl.class);
    
    private final static DoubleWritable dw = new DoubleWritable();
    private Text sourceIp = new Text();
    private String filterDate = null;
    private String delim = null;
    
    int failCount = 0;
    int attemptIndex = 0;
    long inputLength = 0;
    long thisStart = 0;
    private Counter haoQueryBadRowCounter;
    
    protected void setup(Context context
        ) throws IOException, InterruptedException {
      filterDate = context.getConfiguration().get("hao.filterDate");
      delim = context.getConfiguration().get("hao.delim");
      
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
    
      haoQueryBadRowCounter = context.getCounter(TaskCounter.HAO_QUERY_BAD_ROWS);
    }
      
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      

      long currentPos = key.get() - thisStart;
      if (currentPos * (failCount + 1) - inputLength * (attemptIndex + 1) > 0) {
        throw new IOException("HAO FAIL: " + currentPos + "/" + inputLength);
      }
      
      
      StringTokenizer itr;
      if (delim != null) {
        itr = new StringTokenizer(value.toString(), delim);
      } else {
        itr = new StringTokenizer(value.toString());
      }
      try {
        // id field
        itr.nextToken();
        // ip
        sourceIp.set(itr.nextToken());
        // url
        itr.nextToken();
        // date
        String date = itr.nextToken();
        // avrevenue
        dw.set(Double.parseDouble(itr.nextToken()));
        
        if (filterDate == null || date.compareTo(filterDate) >= 0) {
          context.write(sourceIp, dw);
        }
        
      } catch (Exception e) {
        LOG.info("Exception in parse fields: " + e.getMessage());
        haoQueryBadRowCounter.increment(1);
      }
    }
  }
  
  public static class DoubleSumReducer 
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0;
      for (DoubleWritable val : values) {
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
      System.err.println("Usage: haoquery <-Dhao.filterDate=string> <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "HaoQuery");
    job.setJarByClass(HaoQuery.class);
    job.setMapperClass(FieldParser.class);
    job.setCombinerClass(DoubleSumReducer.class);
    job.setReducerClass(DoubleSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
