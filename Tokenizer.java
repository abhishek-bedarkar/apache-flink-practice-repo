package p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class Tokenizer implements MapFunction<String, Tuple2<String,Integer>>{

	public Tuple2<String, Integer> map(String value) throws Exception {
		return new Tuple2<String,Integer>(value,1);
	}

}
