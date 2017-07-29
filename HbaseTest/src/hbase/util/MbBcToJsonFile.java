package hbase.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import hbase.dao.MbBcSearchDao;
import hbase.util.interf.MbBcToJsonFileInterface;
import net.sf.json.JSONObject;

public class MbBcToJsonFile implements MbBcToJsonFileInterface{
	public void mbbcTojsonfile(String goodtype,String brand)  throws IOException, JSONException  {
		MbBcSearchDao bcSearchInterface = new MbBcSearchDao();
		List<Map<String, String>> list = bcSearchInterface.bc_number(goodtype,brand);
		JSONArray ja = new JSONArray();
		FileWriter writer;
		for (Map<String, String> map : list) {
			// 将map转化成json
			/*ObjectMapper mapper = new ObjectMapper();
			String json = "";
			json = mapper.writeValueAsString(map);
			ja.put(json);*/
		JSONObject jsonObject = JSONObject.fromObject(map);
		ja.put(jsonObject);
		//System.out.println("输出的结果是：" + jsonObject);
		}
		String jas = ja.toString();
		try {
			File file = new File("/home/hadoop/jsondata/MB_BC/" + goodtype +"-"+brand+ ".json");
			if(file.exists()){
				writer = new FileWriter("/home/hadoop/jsondata/MB_BC/" + goodtype +"-"+brand+ ".json");
				writer.write(jas);
				System.out.println("写入成功");
				writer.flush();
				writer.close();
			}else{
				if (!file.getParentFile().exists()) {
					// 如果目标文件所在的目录不存在，则创建父目录
					System.out.println("目标文件所在目录不存在，准备创建它！");
					if (!file.getParentFile().mkdirs()) {
						System.out.println("创建目标文件所在目录失败！");
					}
				file.createNewFile();
				writer = new FileWriter("/home/hadoop/jsondata/MB_BC/" + goodtype +"-"+brand+ ".json");
				writer.write(jas);
				System.out.println("写入成功");
				writer.flush();
				writer.close();
			}
		
		}
			} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
