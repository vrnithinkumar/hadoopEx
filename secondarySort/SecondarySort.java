import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySort {

	public static class DayPartitioner extends Partitioner<SecondarySortDayTemperatureKey, NullWritable> {
		@Override
		public int getPartition(SecondarySortDayTemperatureKey key, NullWritable value, int numPartitions) {
			return (key.getDay().hashCode() % numPartitions);
		}
	}

	public static class SecondarySortMapper extends Mapper<Object, Text, SecondarySortDayTemperatureKey, NullWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String tempReadings[] = value.toString().split(";");
			context.write(new SecondarySortDayTemperatureKey(tempReadings[0], Double.parseDouble(tempReadings[3])),
					NullWritable.get());
		}
	}

	public static class SecondarySortReducer extends
			Reducer<SecondarySortDayTemperatureKey, NullWritable, SecondarySortDayTemperatureKey, NullWritable> {
		public void reduce(SecondarySortDayTemperatureKey key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static class DayTemperatureComparator extends WritableComparator {
		public DayTemperatureComparator() {
			super(SecondarySortDayTemperatureKey.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			SecondarySortDayTemperatureKey key1 = (SecondarySortDayTemperatureKey) w1;
			SecondarySortDayTemperatureKey key2 = (SecondarySortDayTemperatureKey) w2;
			int result = key1.getDay().compareTo(key2.getDay());
			if (result != 0) {
				return result;
			}
			return -Double.compare(key1.getTemparature(), key2.getTemparature());
		}
	}

	// Grouping by days
	public static class DayGroupComparator extends WritableComparator {
		public DayGroupComparator() {
			super(SecondarySortDayTemperatureKey.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			SecondarySortDayTemperatureKey key1 = (SecondarySortDayTemperatureKey) w1;
			SecondarySortDayTemperatureKey key2 = (SecondarySortDayTemperatureKey) w2;
			return key1.getDay().compareTo(key2.getDay());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SecondarySort");
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(SecondarySortMapper.class);
		job.setCombinerClass(SecondarySortReducer.class);
		job.setReducerClass(SecondarySortReducer.class);
		job.setMapOutputKeyClass(SecondarySortDayTemperatureKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(DayPartitioner.class);
		job.setSortComparatorClass(DayTemperatureComparator.class);
		job.setGroupingComparatorClass(DayGroupComparator.class);
		job.setOutputKeyClass(SecondarySortDayTemperatureKey.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
