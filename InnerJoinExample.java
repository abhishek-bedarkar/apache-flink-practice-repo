package join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.tuple.*;

public class InnerJoinExample {
	
	public static void main(String args[]) throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
		
		if(params.has("input1") && params.has("input2") &&  params.has("output")) {
			
			env.getConfig().setGlobalJobParameters(params);
			
			DataSet<Tuple3<Integer, String, Integer>> dataLeft = env.readTextFile(params.get("input1")).map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {

				@Override
				public Tuple3<Integer, String, Integer> map(String value) throws Exception {
					String words[] = value.split(",");
					return new Tuple3<Integer, String, Integer>(Integer.parseInt(words[0]), words[1],Integer.parseInt(words[2]));
					};
			
			
			});
			
			
			DataSet<Tuple2<Integer, String>> dataRight = env.readTextFile(params.get("input2")).map(new MapFunction<String, Tuple2<Integer, String>>() {

				@Override
				public Tuple2<Integer, String> map(String value) throws Exception {
					String words[] = value.split(",");
					return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
					};
			
			
			});

			
			DataSet<Tuple3<Integer, String, String>> joinData = dataLeft.join(dataRight).where(2).equalTo(0).with(new JoinFunction< Tuple3<Integer, String, Integer>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {

				@Override
				public Tuple3<Integer, String, String> join(Tuple3<Integer, String, Integer> first,
						Tuple2<Integer, String> second) throws Exception {
					
					return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
				}
			
			
			
			});
			
			
			joinData.writeAsCsv(params.get("output"));
			
			env.execute("Inner join");
		}
		else {
			System.out.println("Invalid arguments to process");
		}
		
	}

}
