import org.apache.hadoop.conf.Configuration; // To configure the job.
import org.apache.hadoop.fs.Path; // To handle file paths in HDFS.
import org.apache.hadoop.io.IntWritable; // Hadoop data type that represents an integer value - specifically designed for use in MapReduce prgorams.
import org.apache.hadoop.mapreduce.Job; // To define and run MapReduce jobs.
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // To read data from HDFS.
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // To write data from HDFS.
import org.apache.hadoop.mapreduce.Job;

public class MovieRatingJob {
    // This is where the MapReduce job is defined and executed.
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(); // Create configuration object, used to store configuration settings for the job.
        Job job = Job.getInstance(conf, "Movie Rating Counter"); // Create new Job instance.
        job.setJarByClass(MovieRatingJob.class); // Set the JAR file containing the job code. This tells Hadoop where to find the classes and methods for the job.
        job.setMapperClass(RatingMapper.class); // This tells Hadoop to use the RatingMapper class as a Mapper for this job.
        job.setCombinerClass(RatingReducer.class); // This tells Hadoop to use the RatingReducer class as a combiner.
        job.setReducerClass(RatingReducer.class); // This tells Hadoop to use the RatingReducer class as the Reducer for this job.
        job.setOutputKeyClass(IntWritable.class); // This tells Hadoop that the output keys of the MapReduce job will be of type IntWritable.
        job.setOutputValueClass(IntWritable.class); // This tells Hadoop that the output values of the MapReduce job will be of type IntWritable.
        FileInputFormat.addInputPath(job, new Path(args[0])); // Specifies the input path for the MapReduce job. args[0] refers to the first command-line argument that you provide when running the job.
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Specifies the output path for the MapReduce job. args[1] refers to the second command-line argument that you provide when running the job.
        System.exit(job.waitForCompletion(true) ? 0 : 1); // Run the MapReduce job and wait for it to complete.
    }
}
