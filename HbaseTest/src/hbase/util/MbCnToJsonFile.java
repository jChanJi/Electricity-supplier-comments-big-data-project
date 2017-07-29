package hbase.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import hbase.dao.MbCnSearchDao;
import net.sf.json.JSONObject;

/**
 * 
 * @author chanji 将MbCnSearchDao的List<Map<String, String>>的数据转换成json放入文件
 *
 */
public class MbCnToJsonFile {
	public void mbcnTojsonfile(String good) throws IOException, JSONException {
		MbCnSearchDao cnSearchInterface = new MbCnSearchDao();
		List<Map<String, String>> list = cnSearchInterface.cn_number(good);
		JSONArray ja = new JSONArray();
		FileWriter writer;
		for (Map<String, String> map : list) {
			// 将map转化成json
			/*
			 * ObjectMapper mapper = new ObjectMapper(); String json = ""; json
			 * = mapper.writeValueAsString(map); ja.put(json);
			 */
			JSONObject jsonObject = JSONObject.fromObject(map);
			ja.put(jsonObject);
			// System.out.println(json);
		}
		String jas = ja.toString();
		// System.out.println(ja.toString());
		// System.out.println(ja);
		try {
			File file = new File("/home/hadoop/jsondata/MB_CN/" + good + ".json");
			if (file.exists()) {
				writer = new FileWriter("/home/hadoop/jsondata/MB_CN/" + good + ".json");
				writer.write(jas);
				System.out.println("写入成功");
				writer.flush();
				writer.close();
			} else {
				if (!file.getParentFile().exists()) {
					// 如果目标文件所在的目录不存在，则创建父目录
					System.out.println("目标文件所在目录不存在，准备创建它！");
					if (!file.getParentFile().mkdirs()) {
						System.out.println("创建目标文件所在目录失败！");
					}
					file.createNewFile();
					writer = new FileWriter("/home/hadoop/jsondata/MB_CN/" + good + ".json");
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
