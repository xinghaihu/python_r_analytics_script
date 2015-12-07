package com.yahoo.ycp.optimizationML.udf;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import junit.framework.TestCase;

public class testFeatureExtractor extends TestCase {
	TupleFactory tf;
	Tuple t;
	
	protected void setUp() {
		tf = TupleFactory.getInstance();
		List<String> ls = Arrays.asList(new String[]{"American Fighter Desota 30inch Vintage Oak Swivel Bar Stool","Furniture","Chairs","1","0","0","0","0","0","0","0","0","0","0","0","0","1","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","1","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","1","0"});
		t = tf.newTuple(ls);
	}
	
	public void testFeatureExtractor() throws IOException {
		Tuple out = new featureExtractor().exec(t);
//		System.out.println(out);
		assertTrue(true);
	}
}
