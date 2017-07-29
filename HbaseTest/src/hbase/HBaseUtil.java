package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;

/**
 * 这个类不用看
 * @param args
 */
public class HBaseUtil {

	private Configuration conf;

	private HBaseAdmin admin;

	public HBaseUtil() throws IOException {
		Configuration cnf = new Configuration();
		this.conf = HBaseConfiguration.create(cnf);
		this.admin = new HBaseAdmin(this.conf);
	}

	public HBaseUtil(Configuration conf) throws IOException {
		this.conf = HBaseConfiguration.create(conf);
		this.admin = new HBaseAdmin(this.conf);
	}
//创建表，输入表名，列名
	public void createTable(String tableName, String columnFamily[]) {
		try {
			if (this.admin.tableExists(tableName)) {
				System.out.println("Table : " + tableName + " already exists !");
			} else {
				@SuppressWarnings("deprecation")
				HTableDescriptor td = new HTableDescriptor(tableName);
				int len = columnFamily.length;
				for (int i = 0; i < len; i++) {
					HColumnDescriptor family = new HColumnDescriptor(columnFamily[i]);
					td.addFamily(family);
				}
				admin.createTable(td);
				System.out.println(tableName + " 表创建成功！");
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(tableName + " 表创建失败！");
		}
	}
//删除表，输入表名
	public void deleteTable(String tableName) {
		try {
			if (this.admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println(tableName + " 表删除成功！");
			} else {
				System.out.println(tableName + " 表不存在！");
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(tableName + " 表删除失败！");
		}
	}
//插入数据，输入表名，行键，列簇，列名，值
	public void insertRecord(String tableName, String rowKey, String columnFamily, String qualifier, String value) {
		try {
			@SuppressWarnings("resource")
			HTable table = new HTable(this.conf, tableName);
			Put put = new Put(rowKey.getBytes());
			put.add(columnFamily.getBytes(), qualifier.getBytes(), value.getBytes());
			table.put(put);
			System.out.println(tableName + " 表插入数据成功！");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(tableName + " 表插入数据失败！");
		}
	}
//删除数据，输入表名，行健名
	public void deleteRecord(String tableName, String rowKey) {
		try {
			@SuppressWarnings("resource")
			HTable table = new HTable(this.conf, tableName);
			Delete del = new Delete(rowKey.getBytes());
			table.delete(del);
			System.out.println(tableName + " 表删除数据成功！");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(tableName + " 表删除数据失败！");
		}
	}
//通过行键获取数据
	public Result getOneRecord(String tableName, String rowKey) {
		try {
			@SuppressWarnings("resource")
			HTable table = new HTable(this.conf, tableName);
			Get get = new Get(rowKey.getBytes());
			Result rs = table.get(get);
		   //byte[] b = rs.getValue("CS".getBytes(),"GC_0".getBytes());
			//String s = new String(b);
			/*System.out.println(s+" 表获取数据成功！");
			System.out.println(tableName + "表获取数据成功！");*/


		 /*   Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(rowKey))); 
			     Scan scan = new Scan();  
		        scan.setFilter(filter);  */
		    
			return rs;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}
	//通过列名的条件获取整行数据数据，输入表名，列名的开头
public ResultScanner getRecordsByColumn(String tableName,String column) throws IOException{
	 @SuppressWarnings("resource")
	HTable table = new HTable(this.conf, tableName);
	  /**
      * QualifierFilter构造函数中第二个参数是ByteArrayComparable类型
      * ByteArrayComparable类有以下子类可以使用：
      * *******************************************
      * BinaryComparator  匹配完整字节数组, 
      * BinaryPrefixComparator  匹配开始的部分字节数组, 
      * BitComparator, 
      * NullComparator, 
      * RegexStringComparator,   正则表达式匹配
      * SubstringComparator
      * *******************************************
      * 下面仅以最可能用到的BinaryComparator、BinaryPrefixComparator举例：
      */
   
	 /* 
	  * 
	  * 只列出包含该值的列
	  * 
	  * 	  * Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL,                   
		        new SubstringComparator(column));*/

	 /*
   * 
   *  //表中存在以column打头的列的名字，过滤结果为所有行的该列数据
             );
       QualifierFilter qualify = new QualifierFilter(
             CompareFilter.CompareOp.EQUAL , 
             new BinaryPrefixComparator(Bytes.toBytes(column))  
	 */
	 
	 //以1:开头的行键的行
	 FilterList filterlist = new FilterList();
	 Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryPrefixComparator(column.getBytes()));
	// Filter filter2 = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator());
	 //Filter filter2  = new QualifierFilter(CompareFilter.CompareOp.NOT_EQUAL, new RegexStringComparator(null));  
	 filterlist.addFilter(filter);
	 //filterlist.addFilter(filter2);
	 Scan scan = new Scan();
     scan.setFilter(filterlist);
     ResultScanner rs = table.getScanner(scan); 
   /*  for(Result r:rs){
    	 System.out.println("Sacnner:"+r);
     }*/
    return  rs;
}	
//获取表中所有数据
	public List<Result> getAllRecords(String tableName) {
		try {
			@SuppressWarnings("resource")
			HTable table = new HTable(this.conf, tableName);
			Scan scan = new Scan();
			ResultScanner scanner = table.getScanner(scan);
			List<Result> list = new ArrayList<Result>();
			for (Result r : scanner) {
				list.add(r);
			}
			scanner.close();
			System.out.println(tableName + " 表获取所有记录成功！");
			return list;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

}
