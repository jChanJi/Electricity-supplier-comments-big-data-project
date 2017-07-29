package hbase.dao.interf;

import java.io.IOException;
import java.util.List;
import java.util.Map;
/*
 * MbBcSearchDao的接口
 * */
public interface MbBcSearchDaoInterface {
	public List<Map<String, String>> bc_number(String goodtype,String brand) throws IOException;
}
