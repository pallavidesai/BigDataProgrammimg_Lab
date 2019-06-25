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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
 

public class Counter  {
	public static class TermCountMapper
	  extends Mapper<LongWritable, Text, Text, IntWritable> {
	  private final IntWritable one = new IntWritable(1);
	  private Text word = new Text();
	  @Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		  
		  String line = value.toString();
		  StringTokenizer itr = new StringTokenizer(line);
		  while (itr.hasMoreTokens()) {
		    word.set(itr.nextToken().toLowerCase());
			   context.write(word, one);
			
		  }
		}
	}
	public static class TermCountOutputReducer
	extends Reducer<Text, IntWritable, Text, IntWritable> {

	private MultipleOutputs<Text, IntWritable> mulOutputs;

	@Override
	public void setup(Context context)
	  throws IOException, InterruptedException {
	   mulOutputs = new MultipleOutputs<Text, IntWritable>(context);
	}
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	      throws IOException, InterruptedException {
		 int TotalValue=0;
		 for (IntWritable value : values) {
		       TotalValue +=value.get();
		    }
			mulOutputs.write(key, new IntWritable(TotalValue), key.toString());
	}
	}
   
   
   
   
	
	public static void main(String[] args) throws Exception {
		
		  Configuration conf = new Configuration();
		  Job job = new Job(conf, "TermCount");
		  job.setJarByClass(Counter.class);
		  job.setMapperClass(TermCountMapper.class);
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(IntWritable.class);
		  
		  job.setReducerClass(TermCountOutputReducer.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(IntWritable.class);
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileInputFormat.addInputPath(job, new Path(args[1]));
		  FileOutputFormat.setOutputPath(job, new Path(args[2]));
		  
		  job.waitForCompletion(true);
		}  
}