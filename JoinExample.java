package p2;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class JoinExample {
	public static void main(String[] args) throws Exception {
		
		final ExecutionEnvironment env  = ExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool param = ParameterTool.fromArgs(args);
		
		env.getConfig().setGlobalJobParameters(param);

		if (param.has("input1") && param.has("input2") && param.has("output")){
			
			DataSet<Tuple2<Integer,String>> employee = env.readTextFile(param.get("input1")).map(new MapFunction<String,Tuple2<Integer,String>>() {
				public Tuple2<Integer,String> map(String value){
					String [] words = value.split(",");
					return new Tuple2<Integer,String> (Integer.parseInt(words[0]),words[1]);
				}
			});
		
		DataSet<Tuple2<Integer,String>> department = env.readTextFile(param.get("input2")).map(new MapFunction<String, Tuple2<Integer,String>>() {

			public Tuple2<Integer, String> map(String value) throws Exception {
				String [] words = value.split(",");
				return new Tuple2<Integer,String>(Integer.parseInt(words[0]),words[1]);
			}
		});
		
		
		DataSet<Tuple3<Integer,String,String>> joined = employee.join(department).where(0).equalTo(0).with(new JoinFunction <Tuple2<Integer,String>,Tuple2<Integer,String>,Tuple3<Integer,String,String>>() {

			public Tuple3<Integer,String,String> join(Tuple2<Integer,String> employee, Tuple2<Integer,String> department) throws Exception {
				
				return new Tuple3<Integer,String,String>(employee.f0,employee.f1,department.f1);
			}
		});
		
		joined.writeAsCsv(param.get("output"),"\n"," ");
		
		env.execute("Join example");
		
		}
		else {
			System.out.println("Input parameter missing ");
		}
		
		
	}

}
