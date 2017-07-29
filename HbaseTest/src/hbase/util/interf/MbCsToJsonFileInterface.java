package hbase.util.interf;

import java.io.IOException;

import org.codehaus.jettison.json.JSONException;
/**
 * 
 * @author chanji
 * 接口文件
 *
 */
public interface MbCsToJsonFileInterface {
	 public void mbcsTojsonfile(String good,String classfy) throws IOException, JSONException;
}
