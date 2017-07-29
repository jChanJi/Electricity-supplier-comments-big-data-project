package hbase.test;

import java.io.IOException;

import org.codehaus.jettison.json.JSONException;

import hbase.util.MbBcToJsonFile;


public class MbBcTest {
	public static void main(String[] args) throws IOException, JSONException {
		MbBcToJsonFile test = new MbBcToJsonFile();
		test.mbbcTojsonfile("奶粉","贝因美");
		
	
	}
}
