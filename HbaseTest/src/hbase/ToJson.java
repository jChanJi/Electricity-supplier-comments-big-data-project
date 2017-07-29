package hbase;

import java.io.IOException;

import org.codehaus.jettison.json.JSONException;

import hbase.util.MbBcToJsonFile;
import hbase.util.MbCnToJsonFile;
import hbase.util.MbCsToJsonFile;

public class ToJson {
public static  void main(String[] args) throws IOException, JSONException {
	// TODO Auto-generated method stub
//MB_CS
/*MbCsToJsonFile cs = new MbCsToJsonFile();
cs.mbcsTojsonfile("奶粉","小于10");*/
/*
MbCnToJsonFile cn = new MbCnToJsonFile();
cn.mbcnTojsonfile("奶粉");*/
MbBcToJsonFile bc  = new MbBcToJsonFile();
bc.mbbcTojsonfile("奶粉", "爱他美");

}
}
