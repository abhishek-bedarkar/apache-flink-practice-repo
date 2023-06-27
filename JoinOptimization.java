package join;


import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.tuple.*;


public class JoinOptimization {
	
	public static void main(String args[]) throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
		
		// check if all mandatory parameters are present
		if(params.has("input1") && params.has("input2") && params.has("output")) {
			
			env.getConfig().setGlobalJobParameters(params);
			
			DataSet<Tuple3<Integer, String, Integer>> leftData = env.readTextFile(params.get("input1")).map(new MapFunction<String ,Tuple3<Integer, String, Integer>>() {
			
				public Tuple3<Integer, String, Integer> map(String value){
					
					String data[] = value.split(",");
					
					return new Tuple3<Integer, String, Integer>(Integer.parseInt(data[0]), data[1],Integer.parseInt(data[2]));			
					
				}
			
			});
			
			
			DataSet<Tuple2<Integer, String>> rightData = env.readTextFile(params.get("input2")).map(new MapFunction<String ,Tuple2<Integer, String>>() {
				
				public Tuple2<Integer, String> map(String value){
					
					String data[] = value.split(",");
					
					return new Tuple2<Integer, String>(Integer.parseInt(data[0]), data[1]);			
					
				}
			
			});
			
			
			// Join optimization
			DataSet<Tuple3<Integer, String, String>> leftJoined = leftData.leftOuterJoin(rightData, JoinHint.BROADCAST_HASH_SECOND).where(2).equalTo(0).with(new JoinFunction<Tuple3<Integer,String,Integer>, Tuple2<Integer,String>, Tuple3<Integer, String, String>>() {
			
				public Tuple3<Integer, String, String> join(Tuple3<Integer, String, Integer> left, Tuple2<Integer, String> right){
					
					if(right == null) {
						return new Tuple3<Integer, String, String>(left.f0, left.f1, "null");
					}
					else {
						return new Tuple3<Integer, String, String>(left.f0, left.f1, right.f1);
					}
				}
			
			});

			
			// Data sink
			leftJoined.writeAsCsv(params.get("output"));
			env.execute("left-join-operation");
			
		}
		else {
			System.out.println("Invalid paramters given\n Required parameters are input1, input2 and output");
		}
		
		
	}


}
