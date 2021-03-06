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
 * Class LogAnalysis allows raw server logs to be processes with a goal of
 * finding the number of failed attempts for each ip address and total number of
 * days over which they occured
 */
public class LogAnalysis {

    /**
     * This class is used to hold information on total failed attempts per ip
     * address and total number of uniqueDates. This class servers as the value on
     * the output of the reducer
     */
    public static class IpStats implements WritableComparable<IpStats> {
        private int total = 0;
        private int uniqueDates = 0;

        public void setTotal(int total) {
            this.total = total;
        }

        public void setUniqueDates(int uniqueDates) {
            this.uniqueDates = uniqueDates;
        }

        public String getTotal() {
            return "" + total;
        }

        public String getUniqueDates() {
            return "" + uniqueDates;
        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(total);
            out.writeInt(uniqueDates);
        }

        public void readFields(DataInput in) throws IOException {
            // t_date.readFields(in);
            // t_ip.readFields(in);
            total = in.readInt();
            uniqueDates = in.readInt();
        }

        public int compareTo(IpStats o) {// Compares first by total failed attempts then total unique days
            if (total != Integer.parseInt(o.getTotal())) {
                return total - Integer.parseInt(o.getTotal());
            }
            return uniqueDates - Integer.parseInt(o.getUniqueDates());
        }

        public int hashCode() {
            String res = total + "" + uniqueDates;
            return res.hashCode();
        }

        public String toString() {
            return total + " " + uniqueDates;
        }

    }

    /**
     * FailedLogin class allows holding of the ip address and date of a failed login
     * server attempt. It servers as the input and output key for the combiner and
     * the output key for the reducer
     */
    public static class FailedLogin implements WritableComparable<FailedLogin> {
        private Text t_date = new Text();
        private Text t_ip = new Text();

        public void setDate(String date) {
            t_date.set(date);
        }

        public void setIP(String ip) {
            t_ip.set(ip);
        }

        public String getDate() {
            return t_date.toString();
        }

        public String getIP() {
            return t_ip.toString();
        }

        public void write(DataOutput out) throws IOException {
            t_date.write(out);
            t_ip.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            t_date.readFields(in);
            t_ip.readFields(in);
        }

        public int compareTo(FailedLogin o) { // Sorts by IP address first then date
            if (this.t_ip.compareTo(o.t_ip) == 0) {
                return this.t_date.compareTo(o.t_date);
            }
            return this.t_ip.compareTo(o.t_ip);
        }


        public int hashCode() {
            return t_date.hashCode();
        }

        public String toString() {
            return t_date.toString() + " " + t_ip.toString();
        }
    }

    /**
     * TokenizerMapper class servers as the mapper class for this Hadoop process. It
     * handles the removal of non failed password entries for root and processes the
     * data (IP address, number of attempts) into their respective class objects
     */
    public static class TokenizerMapper extends Mapper<Object, Text, FailedLogin, IntWritable> {

        private final static IntWritable val = new IntWritable(1);
        private final static FailedLogin failure = new FailedLogin();

        /**
         * Handles the mapping of the data in the right format and excluding non failed root logins
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();

            // Case where it contains "Failed password for root " and "times"
            if (s.contains("Failed password for root") && s.contains("times")) {
                String[] list = s.split(" +");
                String currDate = list[0] + " ";
                if (list[1].length() == 1) {
                    currDate = currDate + " ";
                }
                currDate = currDate + list[1];
                failure.setDate(currDate);
                for (int i = 0; i < list.length; i++) {
                    if (list[i].equals("times")) {
                        int cnt = Integer.parseInt(list[i - 1]);
                        val.set(cnt);
                    }
                    if (list[i].equals("from")) {
                        failure.setIP(list[i + 1]);
                        break;
                    }
                }
                context.write(failure, val);
            }
            // Case where it contains "Failed Failed password for root" only
            else if (s.contains("Failed password for root")) {
                String[] list = s.split(" +");
                String currDate = list[0] + " ";
                if (list[1].length() == 1) {
                    currDate = currDate + " ";
                }
                currDate = currDate + list[1];
                failure.setDate(currDate);

                for (int i = 0; i < list.length; i++) {
                    if (list[i].equals("from")) {
                        failure.setIP(list[i + 1]);
                        break;
                    }
                }
                context.write(failure, val);
            }
            // Case where it contains "more authentication failures" and "user=root"
            else if (s.contains("more authentication failures") && s.contains("user=root")) {
                String[] list = s.split(" +");
                String currDate = list[0] + " ";
                if (list[1].length() == 1) {
                    currDate = currDate + " ";
                }
                currDate = currDate + list[1];
                failure.setDate(currDate);

                for (int i = 0; i < list.length; i++) {
                    if (list[i].equals("more")) {
                        int cnt = Integer.parseInt(list[i - 1]);
                        val.set(cnt);
                    }

                    if (list[i].contains("rhost")) {
                        String currS = list[i];
                        String[] ipID = currS.split("=");
                        failure.setIP(ipID[1]);
                        break;
                    }
                }
                context.write(failure, val);
            }
            // Case where it contains "authentication failure" and "user=root"
            else if (s.contains("authentication failure") && s.contains("user=root")) {
                String[] list = s.split(" +");
                String currDate = list[0] + " ";
                if (list[1].length() == 1) {
                    currDate = currDate + " ";
                }
                currDate = currDate + list[1];
                failure.setDate(currDate);

                for (int i = 0; i < list.length; i++) {
                    if (list[i].contains("rhost")) {
                        String currS = list[i];
                        String[] ipID = currS.split("=");
                        failure.setIP(ipID[1]);
                        break;
                    }
                }
                context.write(failure, val);
            }
        }
    }

    /**
     * IntSumCombiner handles the summing up of failed attempts for each key where
     * the key is a combination of Ip address and date it occured.
     */
    public static class IntSumCombiner extends Reducer<FailedLogin, IntWritable, FailedLogin, IntWritable> {
        private IntWritable result = new IntWritable();

        /**
         * reduce methods handles the combine step
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(FailedLogin key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * IntSumReducer class handles grouping the results of the combiner by their IP
     * address and counts the number of unique dates where each ip has a failed
     * attempt
     */
    public static class IntSumReducer extends Reducer<FailedLogin, IntWritable, FailedLogin, IpStats> {
        private final static FailedLogin totalFailures = new FailedLogin();
        private String currIP = "";
        private int total = 0;
        private int uniqueDates = 0;

        /**
         * reduce methods handles the reduce step
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(FailedLogin key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            String keyIP = key.getIP();
            if (!keyIP.equals(currIP)) {
                if (total != 0) {
                    IpStats result = new IpStats();
                    result.setTotal(total);
                    result.setUniqueDates(uniqueDates);
                    totalFailures.setIP(currIP);
                    totalFailures.setDate("");
                    context.write(totalFailures, result);
                }
                currIP = keyIP;
                total = 0;
                uniqueDates = 0;
            }

            for (IntWritable val : values) {
                int currValue = val.get();
                total += currValue;
            }
            uniqueDates++;
        }

        /**
         * cleanup ensures that the last Ip address is read and included in the result
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void cleanup(Context context) throws IOException, InterruptedException {
            if (total != 0) {
                totalFailures.setIP(currIP);
                totalFailures.setDate("");
                IpStats result = new IpStats();
                result.setTotal(total);
                result.setUniqueDates(uniqueDates);
                context.write(totalFailures, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Log Analysis");
        job.setJarByClass(LogAnalysis.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(FailedLogin.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
