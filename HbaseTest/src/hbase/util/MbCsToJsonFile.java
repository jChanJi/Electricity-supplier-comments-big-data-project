package hbase.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import hbase.dao.MbCsSearchDao;
import hbase.util.interf.MbCsToJsonFileInterface;
import net.sf.json.JSONObject;

/**
 * 
 * @author chanji 类似与上文件
 * 
 * 34行修改了转化的方式
 *40行以下修改了文件生成的路径规则
 *注意需要将MbCsSearchDao的返回类型设置成List<Map<String, String>>，参照MbCsSearchDao
 *生成json文件在hbase.ToJson中进行
 */
public class MbCsToJsonFile implements MbCsToJsonFileInterface {
	@Override
	// good:产品种类，classfy:字数分类
	public void mbcsTojsonfile(String good, String classfy) throws IOException, JSONException {
		MbCsSearchDao cnSearchInterface = new MbCsSearchDao();
		//System.out.println("hello");
		List<Map<String, String>> list = cnSearchInterface.cs_number(good, classfy);
		System.out.println("hello2");
		JSONArray ja = new JSONArray();
		FileWriter writer;
		for (Map<String, String> map : list) {
			// 将map转化成json
			JSONObject jsonObject = JSONObject.fromObject(map);
			ja.put(jsonObject);
			// System.out.println(json);
		}
		String jas = ja.toString();
		// System.out.println(ja.toString());
		try {
			File file = new File("/home/hadoop/jsondata/MB_CS/" + good + "/" + good + "-" + classfy + ".json");
			if (file.exists()) {
				writer = new FileWriter("/home/hadoop/jsondata/MB_CS/" + good + "/" + good + "-" + classfy + ".json");
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
					writer = new FileWriter("/home/hadoop/jsondata/MB_CS/" + good + "/" + good + "-" + classfy + ".json");
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
