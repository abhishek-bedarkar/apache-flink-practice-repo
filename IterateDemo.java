package iter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterateDemo {
	
	public static void main(String args[] ) throws Exception {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<Tuple2<Long, Integer>> data = env.generateSequence(0, 5).map(new MapFunction<Long, Tuple2<Long, Integer>>(){

			@Override
			public Tuple2<Long, Integer> map(Long value) throws Exception {
				return new Tuple2<Long, Integer> (value, 0);
			}			
		});
		
		
	IterativeStream<Tuple2<Long, Integer>> iter = data.iterate(5000);
	
	DataStream<Tuple2<Long, Integer>> plusOne = iter.map(new MapFunction<Tuple2<Long,Integer>, Tuple2<Long, Integer>>() {

		@Override
		public Tuple2<Long, Integer> map(Tuple2<Long, Integer> value) throws Exception {
			if(value.f1 == 10) {
				return value;
			}
			else {
				return new Tuple2<Long, Integer>(value.f0,value.f1+1);
			}
			
		}
	});
	
	
	DataStream<Tuple2<Long,Integer>> lessThanTen = plusOne.filter(new FilterFunction<Tuple2<Long,Integer>>() {
		
		@Override
		public boolean filter(Tuple2<Long, Integer> value) throws Exception {
			if(value.f1 < 10) {
				return true;
			}
			else {
				return false;
			}
		}
	});
	
	
	iter.closeWith(lessThanTen);
	
	DataStream<Tuple2<Long, Integer>> finalData = plusOne.filter(new FilterFunction<Tuple2<Long,Integer>>() {
		
		@Override
		public boolean filter(Tuple2<Long, Integer> value) throws Exception {
			if(value.f1 == 10) {
				return true;
			}
			else {
				return false;
			}
		}
	});	
	
	
	finalData.writeAsText("/home/abhi/Downloads/flink-1.17.1/apps/iterationResult");
		
	}
	
	
}
