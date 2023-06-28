package wc_stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStreaming {
	
	public static void main(String args[]) throws Exception{
		
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(params);
		
		
		DataStream<String> source = env.socketTextStream("localhost", 9999);
		
		DataStream<Tuple2<String, Integer>> count = source.filter( new FilterFunction<String>() {
			
			public boolean filter(String value) {
				
				if(value.toLowerCase().startsWith("a")) {
					return true;
				}
				else {
					return false;
				}
			}
		}).map(new MapFunction<String, Tuple2<String, Integer>>() {

			@Override
			public Tuple2<String, Integer> map(String value) throws Exception {
				
				return new Tuple2<String, Integer>(value,1);
			}
			
			
		}).keyBy(0).sum(1);
		
		
		count.print();
		
		env.execute();
	}

}
