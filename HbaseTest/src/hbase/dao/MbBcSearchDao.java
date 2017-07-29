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
import hbase.dao.interf.MbBcSearchDaoInterface;
/**
 * 
 * MbBc这张表的数据查询，方法返回的是List<Map<Integer, String>> 
 * 的数据，在MbBcToJsonFile中会调用它，将数据转化为json的文件
 */

public class MbBcSearchDao implements MbBcSearchDaoInterface{
	//表的名字
			final static String TABLE_NAME = "MB_BC";
			//针对MB_BC这张表对表中的信息进行提取
		@SuppressWarnings("deprecation")
		
	@Override
	public List<Map<String, String>> bc_number(String goodtype, String brand) throws IOException {
		/*
		 * 种类对应的标号
		 * 奶粉 尿不湿 奶瓶奶嘴 洗发沐浴 宝宝护肤 婴儿推车 安全座椅 婴儿床 孕期营养 婴儿服饰 婴儿玩具
		 *  1         2        3            4         5           6            7         8          9        10          11
		 */
		
		//根据输入的种类确定种类的标号，到数据库进行查询，返回的是map的list对象
		final String[] goods = { "奶粉", "尿不湿", "奶瓶奶嘴", "洗发沐浴", "宝宝护肤", "婴儿推车", "安全座椅", "婴儿床", "孕期营养", "婴儿服饰", "婴儿玩具" };
		int good_index = 0;
		for (int i = 0; i < goods.length; i++) {
			if (goods[i].equals(goodtype)) {
				good_index = ++i;
			}
		}
		int brand_index=0;
		if(good_index==1)
         {
			final String[] brands1 = {"爱他美","贝因美","飞鹤","惠氏“，”美素佳儿","美赞臣","诺优能","雀巢","雅培","伊利"};
			brand_index = 0;
			for (int i = 0; i < brands1.length; i++) {
				if (brands1[i].equals(brand)) {
					brand_index = ++i;
				}
			}
         }
	
		if(good_index==2)
        {
			final String[] brands2 = {"安儿乐","帮宝适","大王","好奇","花王","雀氏","尤妮佳"};
			brand_index = 0;
			for (int i = 0; i < brands2.length; i++) {
				if (brands2[i].equals(brand)) {
					brand_index = ++i;
				}
			}
        }
		if(good_index==3)
        {
			final String[] brands3 = {"NUK","爱得利","贝亲","贝塔","布朗博士","可么多么"};
			brand_index = 0;
			for (int i = 0; i < brands3.length; i++) {
				if (brands3[i].equals(brand)) {
					brand_index = ++i;
				}
			}
        }
		if(good_index==4)
        {
			final String[] brands4 = {"哈罗闪","好孩子","加州宝宝","强生","青蛙王子","施巴"};
			brand_index = 0;
			for (int i = 0; i < brands4.length; i++) {
				if (brands4[i].equals(brand)) {
					brand_index = ++i;
				}
			}
        }
		if(good_index==5)
        {
			final String[] brands5 = {"加州宝宝","妙思乐","强生","施巴","松达","小蜜蜂","郁美净"};
			brand_index = 0;
			for (int i = 0; i < brands5.length; i++) {
				if (brands5[i].equals(brand)) {
					brand_index = ++i;
				}
			}
        }
		if(good_index==6)
        {
			final String[] brands6 = {"Babyruler","Babysing","POUCH","宝宝好","晨辉","好孩子","康贝","小龙哈彼","悠悠"};
			brand_index = 0;
			for (int i = 0; i < brands6.length; i++) {
				if (brands6[i].equals(brand)) {
					brand_index = ++i;
				}
			}
        }
		if(good_index==7)
        {
			final String[] brands7 = {"宝贝第一","感恩","好孩子","惠尔顿","路途乐","迈可适","奇蒂","谐和"};
			brand_index = 0;
			for (int i = 0; i < brands7.length; i++) {
				if (brands7[i].equals(brand)) {
					brand_index = ++i;
				}
			}
        }
		if(good_index==8)
        {
			final String[] brands8 = {"Farska","巴布豆","好孩子","童健","小龙哈彼","小硕士"};
			brand_index = 0;
			for (int i = 0; i < brands8.length; i++) {
				if (brands8[i].equals(brand)) {
					brand_index = ++i;
				}
			}
        }
		if(good_index==9)
        {
			final String[] brands9 = {"Pregnacare","双心","汤臣倍健","天维美"};
			brand_index = 0;
			for (int i = 0; i < brands9.length; i++) {
				if (brands9[i].equals(brand)) {
					brand_index = ++i;
				}
			}
        }
		if(good_index==10)
        {
			final String[] brands10 = {"爬爬","童泰"};
			brand_index = 0;
			for (int i = 0; i < brands10.length; i++) {
				if (brands10[i].equals(brand)) {
					brand_index = ++i;
				}
			}
        }
		if(good_index==11)
        {
			final String[] brands11 = {"澳贝"};
			brand_index = 0;
			for (int i = 0; i < brands11.length; i++) {
				if (brands11[i].equals(brand)) {
					brand_index = ++i;
				}
			}
        }
		ResultScanner rss = getRecordsByColumn(TABLE_NAME, String.valueOf(good_index) + ":"+brand_index+":");
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();

		int i = 0;
		for (Result rs : rss) {
			Map<String, String> map = new HashMap<String, String>();
			for (org.apache.hadoop.hbase.KeyValue kv : rs.list()) {
				
				String value = Bytes.toString(kv.getValue());
			
				if (!value.isEmpty()) {
					i++;
					map.put(String.valueOf(i), value);
				}
				//目前只有每行3个列有数据所以设置为>2
			if (i > 2) {
					i = 0;
				}
			}
			list.add(map);
			System.out.println(map);
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
