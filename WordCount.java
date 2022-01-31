package p1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCount {

	public static void main(String[] args) throws Exception {
		// Create execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// Load parameters
		final ParameterTool param = ParameterTool.fromArgs(args);
		
		// Set Parameter to all envs
		env.getConfig().setGlobalJobParameters(param);
		
		// Load data to dataset 
		DataSet<String> text = env.readTextFile(param.get("input"));
		
		// filter words containing hyphen
		DataSet<String> filteredText = text.filter(new FilterFunction<String>() {
			
			public boolean filter(String value) throws Exception {
				return value.contains("-");
			}
		});
		
		DataSet<Tuple2<String,Integer>> mappedText = filteredText.map(new Tokenizer());
		
		DataSet<Tuple2<String,Integer>> groupedText = mappedText.groupBy(0).sum(1);
		
		
		if(param.has("output")) {
			groupedText.writeAsCsv(param.get("output"),"\n"," ");
			
		}
		env.execute("Wordcount example");
		
	}
}
