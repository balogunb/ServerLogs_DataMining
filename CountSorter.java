import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Class CountSorter allows processed input data on server logs to be sorted in decreasing
 * order by the total number of times the an IP address failed to log in.
 */
public class CountSorter {

    /**
     * This class holds information on the total number of times an IP address
     * failed at login and the IP address itself. It servers as they for the
     * Reducers and allows sorting to occur.
     */
    public static class FailedLogin implements WritableComparable<FailedLogin> {
        private int total = 0; // Holds total number of failed login attempts
        private Text t_ip = new Text(); // Holds Ip address

        public void setTotal(int total) {
            this.total = total;
        }

        public void setIP(String ip) {
            t_ip.set(ip);
        }

        public String getTotal() {
            return "" + total;
        }

        public String getIP() {
            return t_ip.toString();
        }

        public void write(DataOutput out) throws IOException {
            t_ip.write(out);
            out.writeInt(total);
        }

        public void readFields(DataInput in) throws IOException {
            t_ip.readFields(in);
            total = in.readInt();
        }

        public int compareTo(FailedLogin o) {// Compare to first compares by total number of failed login attempt in
                                             // reverse order
            if (this.total == o.total) {
                return o.t_ip.compareTo(this.t_ip);
            }
            return o.total - this.total;
        }

        public int hashCode() {
            return t_ip.hashCode();
        }

        public String toString() {
            return t_ip.toString() + " " + total;
        }
    }

    /**
     * Mapper class for CounterSorter Class. Input: text by line Output:
     * <FailedLogin ,IntWritable>
     */
    public static class SortMapper extends Mapper<Object, Text, FailedLogin, IntWritable> {
        private final static IntWritable val = new IntWritable();
        private final static FailedLogin failure = new FailedLogin();

        /**
         * map handles reorganizing the key and value so it can be sorted correctly 
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            s = s.trim();
            String[] vals = s.split("\\s+");
            int cnt = Integer.parseInt(vals[2]);
            val.set(cnt);
            failure.setIP(vals[0]);
            int tots = Integer.parseInt(vals[1]);
            failure.setTotal(tots);
            context.write(failure, val);
        }

    }

    /**
     * Reducer class for CounterSorter Class. It spits out the same key and value it
     * takes in. At this point the data is already sorted
     * 
     */
    public static class SortReducer extends Reducer<FailedLogin, IntWritable, FailedLogin, IntWritable> {
        private IntWritable result = new IntWritable();


        /**
         * reduce simply spits out the data in the same order it received it
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(FailedLogin key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable val : values) {
                int currValue = val.get();
                total += currValue;
            }
            result.set(total);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Log Analysis");
        job.setJarByClass(CountSorter.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(FailedLogin.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}