import java.io.IOException;
import java.lang.InterruptedException;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;


public class WordCount {

/** 
 * The custom partitioner. 
 */
public static class CustomPartitioner extends Partitioner<Object, Text> {
	@Override
	public int getPartition(Object key, Text value, int numReduceTasks) {
		System.out.println("Custom");
		return 0;
    }
}
	
/**
 * The map class of WordCount.
 */
public static class TokenCounterMapper
    extends Mapper<Object, Text, Text, IntWritable> {
        
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
/**
 * The reducer class of WordCount
 */
public static class TokenCounterReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
/**
 * The main entry point.
 */
public static void main(String[] args) throws Exception {
	
	/*
	//JMX Start
	AdminConnectionFactory acf = new AdminConnectionFactory();
	JMXConnector jmxc = acf.createConnection();
	MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
	ObjectName serviceCfgName = MQObjectName.createServiceConfig("jms");
	jmxc.close();
	//JMX End
	 * */
	
	StorageServiceProxy SSProxy = new StorageServiceProxy("127.0.0.1", 7100, null, null);
	StorageServiceMBean ssmb = (StorageServiceMBean) SSProxy.getSSMB();
	System.out.println(ssmb.getRangeToEndpointMap("cql3_worldcount"));
	
	Murmur3Partitioner partitioner = new Murmur3Partitioner();
	ByteBuffer buf = ByteBuffer.wrap("string".getBytes());
	System.out.println(partitioner.getToken(buf).toString());
	
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = new Job(conf, "WC");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenCounterMapper.class);
    job.setReducerClass(TokenCounterReducer.class);	
    job.setPartitionerClass(CustomPartitioner.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    System.out.println(job.getPartitionerClass());
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}