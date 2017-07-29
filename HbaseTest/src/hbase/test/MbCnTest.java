package hbase.test;

import java.io.IOException;

import org.codehaus.jettison.json.JSONException;

import hbase.util.MbCnToJsonFile;
import hbase.util.MbCsToJsonFile;
/**
 * 
 * @author chanji
 * 用于测试的主函数
 *
 */
public class MbCnTest {
public static void main(String[] args) throws IOException, JSONException {
	MbCnToJsonFile nj = new MbCnToJsonFile();
	nj.mbcnTojsonfile("奶粉");
	
	MbCsToJsonFile sj = new MbCsToJsonFile();
	sj.mbcsTojsonfile("奶瓶奶嘴", "10~20");
}
}
