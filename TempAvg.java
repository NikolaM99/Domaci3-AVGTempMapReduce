package pds;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TempAvg {

	public static class MinMaxCountTuple implements Writable {
		private int min = 0;
		private int max = 0;
		private int MaxCount = 0;
		private int MinCount = 0;
		
		public MinMaxCountTuple() {}
		
		public MinMaxCountTuple(int temperature) {
			this.min = temperature;
			this.max = temperature;
			this.MaxCount = 1;
			this.MinCount=1;
		}
		
		public int getMin() { return min; }
		public int getMax() { return max; }
		
		public int getMaxCount() { return MaxCount; }
		public int getMinCount() { return MinCount; }
		
		public void setMin(int min) { this.min = min; }
		public void setMax(int max) { this.max = max; }
		
		public void setMaxCount(int count) { this.MaxCount = count; }
		public void setMinCount(int count) { this.MinCount = count; }
		
		@Override
		public void readFields(DataInput in) throws IOException {
			min = in.readInt();
			max = in.readInt();
			MaxCount = in.readInt();
			MinCount=in.readInt();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(min);
			out.writeInt(max);
			out.writeInt(MaxCount);
			out.writeInt(MinCount);
			
		}
		
		public String toString() {
			return "min: " + Math.round(min*1.0/MinCount)+ ", max: " + Math.round(max*1.0/MaxCount);
		}
		
	}
	
	public static class TupleMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {
		private Text month = new Text();
		private MinMaxCountTuple outTuple = new MinMaxCountTuple();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(",");
			month.set(line[1].substring(4,6));
			String category=line[2];
			int temperature = Integer.parseInt(line[3]);
			
			if(category.equals("TMAX")) {
			outTuple.setMax(temperature);
			outTuple.setMaxCount(1);
			
			}
			else if(category.equals("TMIN")) {
				outTuple.setMin(temperature);
				outTuple.setMinCount(1);
				
			}
			
			context.write(month, outTuple);
		}
	}
	
	public static class TupleReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
		private MinMaxCountTuple result = new MinMaxCountTuple();		
		
		public void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {
			
			int maxsum=0;
			int minsum=0;
			int CountMin = 0;
			int CountMax=0;
			
			for (MinMaxCountTuple value : values) {
				
				maxsum+=value.getMax();
				minsum+=value.getMin();
				
				CountMax+=value.getMaxCount();
				CountMin+=value.getMinCount();
				
			}
			
			
			result.setMax(maxsum);
			result.setMin(minsum);
			
			result.setMaxCount(CountMax);
			result.setMinCount(CountMin);
			
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "min max count temperature");
		job.setJarByClass(TempAvg.class);
		job.setMapperClass(TupleMapper.class);
		job.setCombinerClass(TupleReducer.class);
		job.setReducerClass(TupleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MinMaxCountTuple.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}

}