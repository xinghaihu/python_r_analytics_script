package com.yahoo.ycp.optimizationML.udf;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class featureExtractor extends EvalFunc<Tuple>{
	int outputFieldNum = 6; 
	int inputFieldNum = 72;
	Logger logger = Logger.getLogger(getClass());
	
	private int getBucket(List<Object> ls ) {
		int i = 0;
		int bucket = 0;
		for (Object ele : ls) {
			bucket += Integer.parseInt(ele.toString()) * i;
			i++;
		}
		return bucket;
	}

	@Override
	public Tuple exec(Tuple input) throws IOException {
		TupleFactory tf = TupleFactory.getInstance();
		try {
			logger.info("the tuple size is: " + input.size());
			if (input.size() != inputFieldNum)
				return null;
			List<Object> inputLs = input.getAll();
			Tuple t = tf.newTuple(outputFieldNum);
			t.set(0, inputLs.get(0));  						// pname
			t.set(1, inputLs.get(2));						// psubcat
			logger.info("the first element are: " + inputLs.subList(3, 16));
			t.set(2, getBucket(inputLs.subList(3, 16)));  	// age
			t.set(3, getBucket(inputLs.subList(16, 19))); 	// gender
			t.set(4, getBucket(inputLs.subList(19, 70))); 	// geo
			t.set(5, inputLs.get(inputLs.size()-1));		// click or not
			return t;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	@Override
	public Schema outputSchema(Schema input) {
		try {
			Schema s = new Schema();
			s.add(new FieldSchema("pName", DataType.CHARARRAY));
			s.add(new FieldSchema("pSubcat", DataType.CHARARRAY));
			s.add(new FieldSchema("ageBucket", DataType.INTEGER));
			s.add(new FieldSchema("genderBucket", DataType.INTEGER));
			s.add(new FieldSchema("geoBucket", DataType.INTEGER));
			s.add(new FieldSchema("click", DataType.INTEGER));
			return s;
		} catch (Exception e) {
			return null;
		}
		
	}
}
