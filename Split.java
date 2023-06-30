import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Split {
	
	public static void main(String[] args) {
		
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(params);
		
		DataStream<String> data = env.readTextFile(params.get("input"));
		
		SplitStream<Integer> split = data.map(new MapFunction<String, Integer>(){)
				
				
			public Integer map(String value){
			
				String price  = value.split(",")[3];
			
				return Integer.parseInt(price);
			
		}
		}.split();
		
	}

}
