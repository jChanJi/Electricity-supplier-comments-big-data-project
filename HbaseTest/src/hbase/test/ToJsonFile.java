package hbase.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONObject;
public class ToJsonFile {
	public static void main(String[] args) {
		  Map map = new HashMap();
	      map.put("msg", "yes");//map里面装有yes
	      JSONObject jsonObject = JSONObject.fromObject(map);
	      System.out.println("输出的结果是：" + jsonObject);
	      //3、将json对象转化为json字符串
	      String result = jsonObject.toString();
	      System.out.println(result);
			FileWriter writer;
			try {
				File file = new File("/home/hadoop/test.json");
				if(file.exists()){
					writer = new FileWriter("/home/hadoop/test.json");
					writer.write(jsonObject.toString());
					System.out.println("写入成功");
					writer.flush();
					writer.close();
				}else{
					file.createNewFile();
					writer = new FileWriter("/home/hadoop/test.json");
					writer.write(jsonObject.toString());
					System.out.println("写入成功");
					writer.flush();
					writer.close();
				}
			
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

}
