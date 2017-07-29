package hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * 这个类不用看
 * @param args
 */
public class HbaseDemo {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {  
        Configuration config = new Configuration();  
        config.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        //config.set("hbase.zookeeper.quorum", "com23.authentication,com22.authentication,com21.authentication");  
        //config.set("hbase.zookeeper.property.clientPort", "2181");  
        //config.set("hbase.master", "192.168.11.179:6000");  
  
   /*     try {  
            HBaseUtil  hbase = new HBaseUtil(config);  
            String tableName = "MB_CS"; 
          //  hbase.getRecords(tableName,"GC_0");
            
           Result result = hbase.getOneRecord(tableName,"10:1:1");
          for(org.apache.hadoop.hbase.KeyValue kv:result.list()){
        	     System.out.println("family:" + Bytes.toString(kv.getFamily()));  
        	     System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));  
        	     System.out.println("value:" + Bytes.toString(kv.getValue()));  
        	     System.out.println("Timestamp:" + kv.getTimestamp());  
        	     System.out.println("-------------------------------------------");  
           		}
       
         }
      catch (IOException e) {  
            e.printStackTrace();  
        }  */
        
       HBaseUtil insert  = new HBaseUtil();
      insert.insertRecord("test","001","ts","tss","ii");
       //insert.deleteRecord("MB_CS","001");
       
          
    }

}