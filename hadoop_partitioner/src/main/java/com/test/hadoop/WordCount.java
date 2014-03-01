package com.test.hadoop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.Map.Entry;
import org.apache.cassandra.thrift.*;

import java.lang.InterruptedException;
import java.util.StringTokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.apache.cassandra.hadoop2.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop2.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop2.ConfigHelper;
import org.apache.cassandra.hadoop2.cql3.CqlConfigHelper;
import org.apache.cassandra.utils.ByteBufferUtil;


public class WordCount {

	 private static final Logger logger = LoggerFactory.getLogger(WordCount.class);

	 static final String KEYSPACE = "cql3_worldcount";
	 static final String COLUMN_FAMILY = "inputs";

	 static final String OUTPUT_REDUCER_VAR = "output_reducer";
	 static final String OUTPUT_COLUMN_FAMILY = "output_words";

	 private static final String OUTPUT_PATH_PREFIX = "/tmp/word_count";
	 private static final String PRIMARY_KEY = "row_key";
/** 
 * The custom partitioner. 
 */
public static class CustomPartitioner extends Partitioner<Object, Text> {
	@Override
	public int getPartition(Object key, Text value, int numReduceTasks) {
		System.out.println("Custom");
		/* Don't create the SSMB/Partitioner objects */
		try {
			StorageServiceProxy SSProxy = new StorageServiceProxy("155.98.39.69", 7100, null, null);
			StorageServiceMBean ssmb = (StorageServiceMBean) SSProxy.getSSMB();
			System.out.println(ssmb.getRangeToEndpointMap("system_traces"));

			Murmur3Partitioner partitioner = new Murmur3Partitioner();
			ByteBuffer buf = ByteBuffer.wrap(key.toString().getBytes());
			System.out.println(partitioner.getToken(buf).toString());
			
		} catch (IOException e) {
			
		}
		
		
		return 0;
    }
}
	
public static class ReducerToCassandra extends Reducer<Text, IntWritable, Map<String, ByteBuffer>, List<ByteBuffer>>
{
    private Map<String, ByteBuffer> keys;
    private ByteBuffer key;
    protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
    throws IOException, InterruptedException
    {
        keys = new LinkedHashMap<String, ByteBuffer>();
        String[] partitionKeys = context.getConfiguration().get(PRIMARY_KEY).split(",");
        keys.put("row_id1", ByteBufferUtil.bytes(partitionKeys[0]));
        keys.put("row_id2", ByteBufferUtil.bytes(partitionKeys[1]));
    }

    public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int sum = 0;
        for (IntWritable val : values)
            sum += val.get();
        context.write(keys, getBindVariables(word, sum));
    }

    private List<ByteBuffer> getBindVariables(Text word, int sum)
    {
        List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
        variables.add(keys.get("row_id1"));
        variables.add(keys.get("row_id2"));
        variables.add(ByteBufferUtil.bytes(word.toString()));
        variables.add(ByteBufferUtil.bytes(String.valueOf(sum)));         
        return variables;
    }
}

public static class TokenizerMapper extends Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, Text, IntWritable>
{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private ByteBuffer sourceColumn;

    protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
    throws IOException, InterruptedException
    {
    }

    public void map(Map<String, ByteBuffer> keys, Map<String, ByteBuffer> columns, Context context) throws IOException, InterruptedException
    {
        for (Entry<String, ByteBuffer> column : columns.entrySet())
        {
            if (!"body".equalsIgnoreCase(column.getKey()))
                continue;

            String value = ByteBufferUtil.string(column.getValue());

            logger.debug("read {}:{}={} from {}",
                         new Object[] {toString(keys), column.getKey(), value, context.getInputSplit()});

            StringTokenizer itr = new StringTokenizer(value);
            while (itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    private String toString(Map<String, ByteBuffer> keys)
    {
        String result = "";
        try
        {
            for (ByteBuffer key : keys.values())
                result = result + ByteBufferUtil.string(key) + ":";
        }
        catch (CharacterCodingException e)
        {
            logger.error("Failed to print keys", e);
        }
        return result;
    }
}

/**
 * The main entry point.
 */
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = new Job(conf, "WC");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(ReducerToCassandra.class);	
    //job.setPartitionerClass(CustomPartitioner.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Map.class);
    job.setOutputValueClass(List.class);
    job.setOutputFormatClass(ColumnFamilyOutputFormat.class);

    ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);
    job.getConfiguration().set(PRIMARY_KEY, "word,sum");
    String query = "INSERT INTO " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY +
                   " (row_id1, row_id2, word, count_num) " +
                   " values (?, ?, ?, ?)";
    CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
    ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "localhost");
    ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
    job.setInputFormatClass(ColumnFamilyInputFormat.class);
    
    ByteBuffer nullByteBuffer = ByteBuffer.wrap("".getBytes());
    SlicePredicate predicate = new SlicePredicate();
    SliceRange sliceRange = new SliceRange(nullByteBuffer, nullByteBuffer, false, 10);
    predicate.setSlice_range(sliceRange);

    ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
    ConfigHelper.setInputInitialAddress(job.getConfiguration(), "127.0.0.1");
    ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
    ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
    ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);
    
    CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3");
    //this is the user defined filter clauses, you can comment it out if you want count all titles
    CqlConfigHelper.setInputWhereClauses(job.getConfiguration(), "title='A'");

    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}