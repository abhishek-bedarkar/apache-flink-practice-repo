import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputExample {
	
	public static void main(String[] args) throws Exception {
		
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(params);
		
		DataStream<Tuple2<String, Integer>> data = env.readTextFile(params.get("input")).map(new MapFunction<String, Tuple2<String, Integer>>(){

			public Tuple2<String, Integer> map(String value) throws Exception{
				
				String feilds[] = value.split(",");
				return new Tuple2<String, Integer>(feilds[0], Integer.parseInt(feilds[3]));
			}
			
			
		});
	
		
		
		final OutputTag<Tuple2<String, Integer>> basicOrder = new OutputTag<Tuple2<String,Integer>>("OrderLessThan50"){};
		final OutputTag<Tuple2<String, Integer>> premiumOrder = new OutputTag<Tuple2<String,Integer>>("OrderMoreThan50"){};
		
		SingleOutputStreamOperator<Tuple2<String, Integer>> processStream = data.process(new ProcessFunction<Tuple2<String,Integer>, Tuple2<String, Integer>>() {


			@Override
			public void processElement(Tuple2<String, Integer> value,
					Context ctx,
					Collector<Tuple2<String, Integer>> arg2) throws Exception {
					
				if(value.f1 <50) {
					ctx.output(basicOrder, value);
				}
				else {
					ctx.output(premiumOrder, value);
				}	
				
			}});
		
		
		DataStream <Tuple2<String, Integer>> basicCustomers = processStream.getSideOutput(basicOrder);
		DataStream <Tuple2<String, Integer>> premiumCustomers = processStream.getSideOutput(premiumOrder);
		
		
		basicCustomers.print();
		premiumCustomers.print();
		
		env.execute("Side output example");
		
		
	}

}
