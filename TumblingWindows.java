package windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TumblingWindows {
	
	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<Tuple2<String, Integer>> data = env.socketTextStream("localhost", 9091).map(new MapFunction<String, Tuple2<String, Integer>>(){

			@Override
			public Tuple2<String, Integer> map(String value) throws Exception {
				String feilds[] = value.split(",");
				String month = feilds[1].split(" ")[0].strip();
				return new Tuple2<String, Integer> (month, Integer.parseInt(feilds[3]));
			}
			
			
			
			
			
		}).keyBy(0)
		.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
		.reduce(new ReduceFunction<Tuple2<String, Integer>>() {

			@Override
			public Tuple2<String, Integer> reduce(Tuple2<String, Integer> current, Tuple2<String, Integer> preResult)
					throws Exception {
				
				return new Tuple2<String, Integer>(current.f0, current.f1 + preResult.f1);
			}
		});
		
		
		data.writeAsCsv("/home/abhi/Downloads/flink-1.17.1/apps/tumbling_result",WriteMode.OVERWRITE);
		
		env.execute("tumbling windows");		
	}

}
