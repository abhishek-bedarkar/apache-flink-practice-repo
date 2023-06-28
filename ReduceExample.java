package reduce;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceExample {
	public static void main(String args[]) throws Exception {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
		
		
		DataStream<Tuple3<String, Integer, Integer>> monthySalesAverage = env.readTextFile(params.get("input")).map(new MapFunction<String,Tuple3<String, Integer, Integer>>(){
			
			public Tuple3<String, Integer, Integer> map(String data){
				
				String feilds[] = data.split(",");
				String month = feilds[1].replace("\"", "").split(" ")[0].strip();
				
				return new Tuple3<String, Integer, Integer>(month, Integer.parseInt(feilds[3]), 1);
				
			}
			
		}).keyBy(0).reduce(new ReduceFunction<Tuple3<String,Integer,Integer>>() {
			
			@Override
			public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> current,
					Tuple3<String, Integer, Integer> pre_result) throws Exception {
				
				return new Tuple3<String, Integer, Integer>(current.f0, current.f1 + pre_result.f1, current.f2+ pre_result.f2);
			}
		}).map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>(){
			
			public Tuple3<String, Integer, Integer> map(Tuple3<String, Integer, Integer> input){
				
				return new Tuple3<String, Integer, Integer>(input.f0, input.f1, new Integer(input.f1/input.f2));
			}
			
		});
		
		
		monthySalesAverage.print();
		env.execute("Calculate monthly sales average");
		
		
		
		
	}

}
