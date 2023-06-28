package aggregation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;

public class Aggregation {

	
public static void main(String args[]) throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
		
		
		DataSet<Tuple4<String, String, String, Integer>> mapped = env.readTextFile(params.get("input")).map(new MapFunction<String,Tuple4<String, String, String, Integer>>(){
			
			public Tuple4<String, String, String, Integer> map(String data){
				
				String feilds[] = data.split(",");
				String month = feilds[1].replace("\"", "").split(" ")[0].strip();
				return new Tuple4<String, String, String, Integer>(feilds[0], month, feilds[2], Integer.parseInt(feilds[3]));
				
			}
			
		});
		
		mapped.groupBy(1).sum(3).writeAsText(params.get("output")+"sum");
		mapped.groupBy(1).min(3).writeAsText(params.get("output")+"min");
		mapped.groupBy(1).max(3).writeAsText(params.get("output")+"max");
		mapped.groupBy(1).minBy(3).writeAsText(params.get("output")+"minBy");
		mapped.groupBy(1).maxBy(3).writeAsText(params.get("output")+"maxBy");
		
		
		env.execute("Sales data profiling");
		
}

}
