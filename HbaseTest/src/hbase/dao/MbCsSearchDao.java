package hbase.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import hbase.HBaseUtil;
import hbase.dao.interf.MbCsSearchDaoInterface;
/**
 * 
 * @author chanji
 * 和MbCnSearchDao原理相同
 *
 */
public class MbCsSearchDao implements MbCsSearchDaoInterface {
	//表的名字
		final static String TABLE_NAME = "MB_CS";
		//针对MB_CN这张表对表中的信息进行提取
	@SuppressWarnings("deprecation")
	@Override
	public List<Map<String, String>> cs_number(String good, String classfy) throws IOException {

		/*
		 * 种类对应的标号
		 * 奶粉 尿不湿 奶瓶奶嘴 洗发沐浴 宝宝护肤 婴儿推车 安全座椅 婴儿床 孕期营养 婴儿服饰 婴儿玩具
		 *  1         2        3            4         5           6            7         8          9        10          11
		 */
		
		//根据输入的种类确定种类的标号，到数据库进行查询，返回的是map的list对象
		final String[] goods = { "奶粉", "尿不湿", "奶瓶奶嘴", "洗发沐浴", "宝宝护肤", "婴儿推车", "安全座椅", "婴儿床", "孕期营养", "婴儿服饰", "婴儿玩具" };
		int good_index = 0;
		for (int i = 0; i < goods.length; i++) {
			if (goods[i].equals(good)) {
				good_index = ++i;
			}
		}
		
		//根据字数分类的标号对数据进行分类
		/*
		 * 小于10	10~20	大于20
		        1	        2	        3
		 */
		final String[] brands = {"小于10","10~20","大于20"};
		int  brand_index = 0;
		for (int i = 0; i < brands.length; i++) {
			if (brands[i].equals(classfy)) {
				brand_index = ++i;
			}
		}
		
		/*System.out.println("good:"+good_index);
		System.out.println("bradd:"+brand_index);*/
		
		//System.out.println(brand_index);
		
		ResultScanner rss = getRecordsByColumn(TABLE_NAME, String.valueOf(good_index) + ":"+brand_index+":");
		
		//System.out.println(rss);
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();

		int i = 0;
		for (Result rs : rss) {
			Map<String, String> map = new HashMap<String, String>();
			for (org.apache.hadoop.hbase.KeyValue kv : rs.list()) {
				// System.out.println("key:"+Bytes.toString(kv.getKey())+"value:" + Bytes.toString(kv.getValue()));
				String value = Bytes.toString(kv.getValue());
			
				if (!value.isEmpty()) {
					// System.out.println(value);
					++i;
					map.put(String.valueOf(i), value);
				}
				//目前只有每行2个列有数据所以设置为>1
			if (i > 1) {
					i = 0;
				}
			}
			list.add(map);
			//System.out.println(map);
		}
	
		return list;
	}
	//返回ResultScanner的数据集，根据rowkey的前缀进行筛选，column为rowkey的前缀
	public static ResultScanner getRecordsByColumn(String tableName, String column) throws IOException {
		Configuration conf = new Configuration();
		conf.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
		HBaseUtil hbase = new HBaseUtil(conf);
		System.out.println(tableName + ":" + column);
		return  hbase.getRecordsByColumn(tableName, column);
}
}
