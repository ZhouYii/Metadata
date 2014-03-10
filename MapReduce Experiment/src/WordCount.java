import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.hadoop2.ConfigHelper;
import org.apache.cassandra.hadoop2.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop2.cql3.CqlOutputFormat;
import org.apache.cassandra.hadoop2.cql3.CqlPagingInputFormat;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
//NEED TO USE THE NEW JAR
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount {
	public static final String SEED_CASSANDRA_SERVER = "127.0.0.1";

	private static final Logger logger = LoggerFactory
			.getLogger(WordCount.class);

	static final String KEYSPACE = "cql3_worldcount";
	static final String COLUMN_FAMILY = "inputs";

	static final String OUTPUT_REDUCER_VAR = "output_reducer";
	static final String OUTPUT_COLUMN_FAMILY = "output_words_2";

	private static final String OUTPUT_PATH_PREFIX = "/tmp/word_count";

	private static final String PRIMARY_KEY = "row_key";

	/**
	 * The custom partitioner.
	 */
	public static class CustomPartitioner extends
			Partitioner<Text, IntWritable> implements JobConfigurable {
		private static String[] ring_topology;
		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			System.out.println("Custom");
			Murmur3Partitioner partitioner = new Murmur3Partitioner();
			ByteBuffer buf = ByteBuffer.wrap(key.toString().getBytes());
			int tok = partitioner.getToken(buf).getToken().hashCode();
			
			// Iterate over map to find relevant list of endpoints for each
			// token
			for (int i =0 ; i < ring_topology.length ; i++) {
				String[] parts = ring_topology[i].split("##");
				Long lbound = Long.parseLong(parts[0]);
				Long rbound = Long.parseLong(parts[1]);
				if (tok >= lbound && tok <= rbound) {
					return i;
				}
			}
			return 0;
		}

		@Override
		public void configure(JobConf conf) {
			ring_topology = conf.get("mapreduce.job.reduce.locality.hints").split("//");	
		}
	}

	public static class TokenizerMapper
			extends
			Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private ByteBuffer sourceColumn;

		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
		}

		public void map(Map<String, ByteBuffer> keys,
				Map<String, ByteBuffer> columns, Context context)
				throws IOException, InterruptedException {
			for (Entry<String, ByteBuffer> column : columns.entrySet()) {
				if (!"body".equalsIgnoreCase(column.getKey()))
					continue;

				String value = ByteBufferUtil.string(column.getValue());

				logger.debug("read {}:{}={} from {}",
						new Object[] { toString(keys), column.getKey(), value,
								context.getInputSplit() });

				StringTokenizer itr = new StringTokenizer(value);
				while (itr.hasMoreTokens()) {
					word.set(itr.nextToken());
					context.write(word, one);
				}
			}
		}

		private String toString(Map<String, ByteBuffer> keys) {
			String result = "";
			try {
				for (ByteBuffer key : keys.values())
					result = result + ByteBufferUtil.string(key) + ":";
			} catch (CharacterCodingException e) {
				logger.error("Failed to print keys", e);
			}
			return result;
		}
	}

	public static class ReducerToCassandra
			extends
			Reducer<Text, IntWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
		private Map<String, ByteBuffer> keys;
		private ByteBuffer key;

		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			keys = new LinkedHashMap<String, ByteBuffer>();
			String[] partitionKeys = context.getConfiguration()
					.get(PRIMARY_KEY).split(",");
			keys.put("row_id1", ByteBufferUtil.bytes(partitionKeys[0]));
			keys.put("row_id2", ByteBufferUtil.bytes(partitionKeys[1]));
		}

		public void reduce(Text word, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values)
				sum += val.get();
			context.write(keys, getBindVariables(word, sum));
		}

		private List<ByteBuffer> getBindVariables(Text word, int sum) {
			List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
			keys.put("word", ByteBufferUtil.bytes(word.toString()));
			variables.add(ByteBufferUtil.bytes(String.valueOf(sum)));
			return variables;
		}
	}

	public static JobConf InitKeyTopology(JobConf conf) throws IOException {
		StorageServiceProxy SSProxy = new StorageServiceProxy(SEED_CASSANDRA_SERVER, 7100, null, null);
		StorageServiceMBean ssmb = (StorageServiceMBean) SSProxy.getSSMB();
		Map<Range<Token>, List<String>> tok_ring = ssmb.getRangeToEndpointMap(KEYSPACE);
		String tok_ring_str = topology_to_string(tok_ring, conf);
		conf.set("mapreduce.job.reduce.locality.hints", tok_ring_str);
		return conf;
	}

	private static String topology_to_string(Map<Range<Token>, List<String>> tok_ring, JobConf conf) {
		ArrayList<String> localityHints = new ArrayList<String>();
		String accumulator = "";
		String tok_str = tok_ring.toString();
		tok_str = tok_str.substring(0, tok_str.length()-2);
		String[] parts = tok_str.split("], ");
		for(String part : parts) {
			String[] tokens = part.split("=");
			accumulator = accumulator.concat(fetch_left_range(tokens[0])+"##");
			accumulator = accumulator.concat(fetch_right_range(tokens[0])+"##");
			String host = tokens[1].substring(1);
			accumulator = accumulator.concat(host+"//");
			localityHints.add(host);
		}
		insert_locality_hints(localityHints, conf);
		return accumulator;
	}
	private static void insert_locality_hints(ArrayList<String> localityHints, JobConf conf) {
		int curr = 0;
		int wrap = localityHints.size();
		while(localityHints.size()<conf.getNumReduceTasks()) {
			localityHints.add(localityHints.get(curr%wrap));
			curr++;
		}
		conf.setStrings("mapreduce.job.reduce.locality.hints", (String[]) localityHints.toArray());
	}

	private static String fetch_left_range(String str) {
		int lbound = str.indexOf("[")+1;
		int rbound = str.indexOf(',');
		return str.substring(lbound, rbound);
	}
	private static String fetch_right_range(String str) {
		int lbound = str.indexOf(" ")+1;
		int rbound = str.indexOf(']');
		return str.substring(lbound, rbound);
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(WordCount.class);
		conf = InitKeyTopology(conf);
		Job job = new Job(conf, "wordcount");
		conf.setNumReduceTasks(4);
		
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(ReducerToCassandra.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Map.class);
		job.setOutputValueClass(List.class);
		job.setOutputFormatClass(CqlOutputFormat.class);

		
		ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE,
				OUTPUT_COLUMN_FAMILY);
		job.getConfiguration().set(PRIMARY_KEY, "word,sum");
		String query = "UPDATE " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY
				+ " SET count_num = ? ";

		CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
		ConfigHelper.setOutputInitialAddress(job.getConfiguration(),
				"localhost");
		ConfigHelper.setOutputPartitioner(job.getConfiguration(),
				"Murmur3Partitioner");

		job.setInputFormatClass(CqlPagingInputFormat.class);

		ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
		ConfigHelper.setInputInitialAddress(job.getConfiguration(),
				SEED_CASSANDRA_SERVER);
		ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE,
				COLUMN_FAMILY);
		ConfigHelper.setInputPartitioner(job.getConfiguration(),
				"Murmur3Partitioner");

		CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3");
		CqlConfigHelper.setInputWhereClauses(job.getConfiguration(),
				"title='A'");
		job.waitForCompletion(true);
	}

}