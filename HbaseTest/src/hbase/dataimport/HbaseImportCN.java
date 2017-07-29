
package hbase.dataimport;

import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
/**
 * 
 * @author Chanji
 * 
 *MB_CN表的插入，每次运行需要修改种类
 */
public class HbaseImportCN {
/*
	 * 奶粉	尿不湿	奶瓶奶嘴  	洗发沐浴      	宝宝护肤  	婴儿推车	  安全座椅	婴儿床	孕期营养 	婴儿服饰	  婴儿玩具
	      1	         2             	3	          4	                 5           	6	           7	       8	         9	            10	          11
	*/
	static enum Num{
		exNum
	}
	//需要定义的变量
	 static final String INPUT_PATH = "hdfs://localhost:9000/procducedata/婴儿玩具1/part-r-00000";//文件路径，要改
	 static final String GT = "婴儿玩具";//商品种类，要改
	 static final String GS = "11";//种类标号，要改
	 static final String DATABASE_NAME = "MB_CN";//表名，不用改
	 static final String COLUMN_FAMILY = "CN";//列簇名，不用改
	 static final String ROOTDIR = "hdfs://localhost:9000/hbase";//hbase.env.sh中hbase.rootdir的配置，不用改
	
	 
	 
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
		final	Configuration conf = new Configuration();
		conf.set("hbase.rootdir", ROOTDIR);
	//	conf.set("hbase.zookeeper.quorum","localhost");
		//表名
		conf.set(TableOutputFormat.OUTPUT_TABLE,DATABASE_NAME);
		conf.set("dfs.socket.timeout", "180000");
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,HbaseImportCN.class.getSimpleName());
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setMapperClass(Map.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(HbaseImportCN.class);
		job.setReducerClass(Reduce.class);
		
		//直接创建表 和 导入数据 到hbase里面 所以不需要指定 输出文件路径 输出reducer类型
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass( TableOutputFormat.class);
		
		job.waitForCompletion(true);
	}
	static class Map extends Mapper <LongWritable , Text , LongWritable , Text >{
		//时间格式
	//	SimpleDateFormat format1 = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
		private Text v2 = new Text();
		int i = 0;
		protected void map(LongWritable key , Text value , Context context) throws IOException, InterruptedException{
			final String[] splited = value.toString().split(" ");
			try{
				//final Date date = new Date(Long.parseLong(splited[0].trim()));
				//final String dateFormat = format1.format(date);
				String rowkey = GS+":"+String.valueOf(++i).trim();	//行键
				v2.set(rowkey + "\t" + value.toString());
				context.write(key, v2);
			}catch(NumberFormatException e){
				final Counter counter = context.getCounter(Num.exNum);
				counter.increment(1L);
				System.out.println("出错"+splited[0]+" "+e.getMessage());
			}		
		}
	}
	//注意是TableReducer
	static class Reduce extends TableReducer <LongWritable , Text , NullWritable>{
		protected void reduce(LongWritable key , Iterable<Text>v2s, Context context) throws IOException, InterruptedException{
			for (Text v2 : v2s) {
				String[] splited = v2.toString().split("\t");
				for(String s:splited){
					System.out.println(s);
				}
				String rowkey = splited[0];
			
				System.out.println(rowkey);
				Put put = new Put(rowkey.getBytes());
				put.add(COLUMN_FAMILY.getBytes(), Bytes.toBytes("GT"),Bytes.toBytes(GT.trim()));
   			put.add(COLUMN_FAMILY.getBytes(), Bytes.toBytes("GB"), Bytes.toBytes(splited[1].trim()));
       		System.out.println("GB"+splited[1].trim());
				
				//splited[2]是空行
				put.add(COLUMN_FAMILY.getBytes(), Bytes.toBytes("BTC"), Bytes.toBytes(splited[3].trim()));
				System.out.println("BTC"+splited[3].trim());
				put.add(COLUMN_FAMILY.getBytes(), Bytes.toBytes("BJC"), Bytes.toBytes(splited[4].trim()));
				System.out.println("BJC"+splited[4].trim());
				put.add(COLUMN_FAMILY.getBytes(), Bytes.toBytes("BCS"), Bytes.toBytes(splited[5].trim()));
				System.out.println("BCS"+splited[5].trim());
				put.add(COLUMN_FAMILY.getBytes(), Bytes.toBytes("GNAME"), null);
				put.add(COLUMN_FAMILY.getBytes(), Bytes.toBytes("GLINK"), null);
				put.add(COLUMN_FAMILY.getBytes(), Bytes.toBytes("PLATFORM"), null);
				put.add(COLUMN_FAMILY.getBytes(), Bytes.toBytes("REMARKS"), null);
				context.write(NullWritable.get(),put);
			}

		}
	}
}