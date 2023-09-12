import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UrlCount {

  public static class UrlMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text urlText = new Text();
    private final static IntWritable one = new IntWritable(1);
    // Regular expression to match URLs
    private static final String URL_REGEX = "href=\"(https?://[^\"]+)\"";
    private Pattern pattern;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      pattern = Pattern.compile(URL_REGEX);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      Matcher matcher = pattern.matcher(line);

      while (matcher.find()) {
        String url = matcher.group(1);
        urlText.set(url);
        context.write(urlText, one);
      }
    }
  }

  public static class UrlReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);

      // Output URLs with more than 5 references
      if (sum > 5) {
        context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "url count");
    job.setJarByClass(UrlCount.class);
    job.setMapperClass(UrlMapper.class);
    job.setReducerClass(UrlReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}




















//------------------------------------------------------

// import java.io.IOException;
// import java.util.StringTokenizer;
// import java.util.regex.Matcher;
// import java.util.regex.Pattern;

// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.LongWritable; 
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// public class UrlCount {

//   public static class UrlMapper extends Mapper<Object, Text, Text, LongWritable> {
//     private Text urlText = new Text();
//     private final static LongWritable one = new LongWritable(1);
//     // Regular expression to match URLs
//     private static final String URL_REGEX = "href=\"(https?://[^\"]+)\"";
//     private Pattern pattern;

//     public void setup(Context context) {
//       pattern = Pattern.compile(URL_REGEX);
//     }

//     public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//       String line = value.toString();
//       Matcher matcher = pattern.matcher(line);

//       while (matcher.find()) {
//         String url = matcher.group(1);
//         urlText.set(url);
//         context.write(urlText, one);
//       }
//     }
//   }

//   public static class UrlReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
//     private LongWritable result = new LongWritable();

//     public void reduce(Text key, Iterable<LongWritable> values, Context context)
//         throws IOException, InterruptedException {
//       long sum = 0;
//       for (LongWritable val : values) {
//         sum += val.get();
//       }
//       result.set(sum);

//       //output URLs with more than 5 references
//       if (sum > 5) {
//         context.write(key, result);
//       }
//     }
//   }

//   public static void main(String[] args) throws Exception {
//     Configuration conf = new Configuration();
//     Job job = Job.getInstance(conf, "url count");
//     job.setJarByClass(UrlCount.class);
//     job.setMapperClass(UrlMapper.class);
//     job.setReducerClass(UrlReducer.class);
//     job.setOutputKeyClass(Text.class);
//     job.setOutputValueClass(LongWritable.class);
//     FileInputFormat.addInputPath(job, new Path(args[0]));
//     FileOutputFormat.setOutputPath(job, new Path(args[1]));
//     System.exit(job.waitForCompletion(true) ? 0 : 1);
//   }
// }
