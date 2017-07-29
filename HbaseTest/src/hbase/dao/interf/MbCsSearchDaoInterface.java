package hbase.dao.interf;

import java.io.IOException;
import java.util.List;
import java.util.Map;
/**
 * 
 * @author chanji
 * MbCsSearchDao的接口
 *
 */
public interface MbCsSearchDaoInterface {
	public List<Map<String, String>> cs_number(String good,String classfy) throws IOException;
}
