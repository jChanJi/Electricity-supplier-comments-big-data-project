package hbase.util.interf;

import java.io.IOException;

import org.codehaus.jettison.json.JSONException;

public interface MbBcToJsonFileInterface {
	public void mbbcTojsonfile(String goodtype,String brand) throws IOException, JSONException;
}
