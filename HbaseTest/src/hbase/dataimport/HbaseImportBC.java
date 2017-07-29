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

import hbase.dataimport.HbaseImportBC.Map.Reduce;

/*
 * MB_BC表的插入，每次插入需要修改商品种类，品牌等信息
 * */
public class HbaseImportBC {
	/*
	 * 种类标号
	 * 奶粉	尿不湿	奶瓶奶嘴  	洗发沐浴      	宝宝护肤  	婴儿推车	  安全座椅	婴儿床	孕期营养 	婴儿服饰	  婴儿玩具
	      1	         2             	3	          4	                 5           	6	           7	       8	         9	            10	          11
	    每一个种类下的品牌标号也是从1开始，这里就不一一列出，具体看数据库见表规范中的备注
	    这里以奶粉为例 
	    爱他美   贝因美     飞鹤       惠氏         美素佳儿       美赞臣  诺优能       雀巢        雅培      伊利 
 	        1         2           3           4               5                 6        7             8            9         10 
	*/
	
	 static final String INPUT_PATH = "hdfs://localhost:9000/procducedata/婴儿玩具2/澳贝/-r-00000";//文件路径，要改
	 static final String GT = "婴儿玩具";//商品种类，要改
	 static final String GS = "11";//种类标号，要改
	 static final String GB = "澳贝";//商品品牌，要改       
	 static final String BS = "1";//品牌标号，要改
	 static final String BC = "BC";
	 static final String DATABASE_NAME = "MB_BC";//表名，不用改
	 static final String COLUMN_FAMILY = "BC";//列簇名，不用改
	 static final String ROOTDIR = "hdfs://localhost:9000/hbase";//hbase.env.sh中hbase.rootdir的配置，不用改
	 static enum Num{
		exNum
	 }
	 public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
			final	 Configuration conf = new Configuration();
			conf.set("hbase.rootdir", ROOTDIR);
		//	conf.set("hbase.zookeeper.quorum","localhost");
			//表名
			conf.set(TableOutputFormat.OUTPUT_TABLE,DATABASE_NAME);
			conf.set("dfs.socket.timeout", "180000");
			
			@SuppressWarnings("deprecation")
			Job job = new Job(conf,HbaseImportBC.class.getSimpleName());
			FileInputFormat.setInputPaths(job, INPUT_PATH);
			job.setMapperClass(Map.class);
			
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setJarByClass(HbaseImportBC.class);
			job.setReducerClass(Reduce.class);
			
			//直接创建表 和 导入数据 到hbase里面 所以不需要指定 输出文件路径 输出reducer类型
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TableOutputFormat.class);
			
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
					String rowkey = GS+":"+BS+":"+String.valueOf(++i).trim();	//行键为种类标号(GS）：品牌标号（BS):，在上面的注释中有写
					v2.set(rowkey + "\t" + value.toString());
					context.write(key, v2);
				}catch(NumberFormatException e){
					final Counter counter = context.getCounter(Num.exNum);
					counter.increment(1L);
					System.out.println("出错"+splited[0]+" "+e.getMessage());
				}		
			}
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
						put.add(COLUMN_FAMILY.getBytes(), Bytes.toBytes("GB"),Bytes.toBytes(GB.trim()));
						//splited[1]为空行
						put.add(COLUMN_FAMILY.getBytes(), Bytes.toBytes(BC), Bytes.toBytes(splited[2].trim()));
						System.out.println(BC+splited[2].trim());
						put.add(COLUMN_FAMILY.getBytes(), Bytes.toBytes("GNAME"), null);
						put.add(COLUMN_FAMILY.getBytes(), Bytes.toBytes("GLINK"), null);
						put.add(COLUMN_FAMILY.getBytes(), Bytes.toBytes("PLATFORM"), null);
						put.add(COLUMN_FAMILY.getBytes(), Bytes.toBytes("REMARKS"), null);
						context.write(NullWritable.get(),put);
					}

				}
			}
		}
}

