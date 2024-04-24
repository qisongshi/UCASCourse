import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Hw2Part1 {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, DoubleWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(" ");
      if(tokens.length == 3)
      {
        word.set(tokens[0] + " " + tokens[1]);
        DoubleWritable time = new DoubleWritable(Double.valueOf(tokens[2]));
        context.write(word, time);
      }
    }
  }
  
  public static class DoubleSumReducer
       extends Reducer<Text,DoubleWritable,Text,Text> {

    private Text result_key= new Text();
    private Text result_value= new Text();

    protected void setup(Context context) {
    }

    public void reduce(Text key, Iterable<DoubleWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0;
      int count = 0;
      for (DoubleWritable val : values) {
        sum += val.get();
        count++;
      }
      result_key.set(key);

      // generate result value
      double avg = sum / count;
      String count_str = String.valueOf(count);
      String avg_str = String.format("%.3f", avg);
      result_value.set(count_str + " " + avg_str);

      context.write(result_key, result_value);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: <input file> <output directory>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "Hw2Part1");

    job.setJarByClass(Hw2Part1.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(DoubleSumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // add the input paths as given by command line
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }

    // add the output path as given by the command line
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
