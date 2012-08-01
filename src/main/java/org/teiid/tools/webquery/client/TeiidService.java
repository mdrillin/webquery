package org.teiid.tools.webquery.client;

import java.util.List;
import java.util.Map;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("teiid")
public interface TeiidService extends RemoteService {
	
	  String[] getAllDataSourceNames(boolean teiidOnly) throws Exception;
	  
	  Map<String,List<String>> getTableAndColMap(String dataSource);

	  List<List<DataItem>> executeSql(String dataSource, String sql) throws Exception;  
}
