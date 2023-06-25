package wc;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCount {
	
	public static void main(String args[]) throws Exception {
		
		// Create local environment
		final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		
		// Read params from args
		final ParameterTool params = ParameterTool.fromArgs(args);
		
		// set parameters as global
		env.getConfig().setGlobalJobParameters(params);
		
		//read text file
		DataSet<String> text = env.readTextFile(params.get("inputFile"));
		
		//Filter records with name starting with A
		DataSet<String> filteredText = text.filter(new FilterFunction<String>() {
			
			public boolean filter(String name) {
				return name.contains(params.get("containsKey"));
			}
			
		});
		
		// Tokenize records to get count
		
		DataSet<Tuple2<String, Integer>> tokenized = filteredText.map(new Tokenizer());
		
		DataSet<Tuple2<String, Integer>> wordCount = tokenized.groupBy(0).sum(1);
		
		if(params.has("outputFilePath")) {
			
			wordCount.writeAsCsv(params.get("outputFilePath"));
			
			env.execute("Word count application");
		}
		
		
		
	}
	
	public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>>{ 	
		
		public Tuple2<String, Integer> map(String data){
			return new Tuple2<String, Integer>(data.toLowerCase(),1);
		}
	}

}
