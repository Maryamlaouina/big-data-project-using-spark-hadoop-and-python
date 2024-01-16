import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class UniqueUserMovies2019 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text movieId = new Text();
        private Text userId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");

            // Assuming line format is UserID, MovieID, Date
            String userID = parts[0];
            String movieID = parts[1];
            String date = parts[2];

            // Check if the date is in 2019
            if (date.contains("2019")) {
                movieId.set(movieID);
                userId.set(userID);
                context.write(movieId, userId);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> uniqueUsers = new HashSet<>();

            for (Text val : values) {
                uniqueUsers.add(val.toString());
            }

            // Write the movie to output only if a single user watched it
            if (uniqueUsers.size() == 1) {
                result.set(uniqueUsers.iterator().next());
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "single viewer movies 2019");
        job.setJarByClass(UniqueUserMovies2019.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
