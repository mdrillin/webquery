package org.teiid.tools.webquery.server;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.sql.rowset.serial.SerialBlob;

import org.teiid.tools.webquery.client.DataItem;
import org.teiid.tools.webquery.client.SQLProcException;
import org.teiid.tools.webquery.client.TeiidService;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;

/**
 * The server side implementation of the RPC service.
 */
@SuppressWarnings("serial")
public class TeiidServiceImpl extends RemoteServiceServlet implements TeiidService {

	private static final String WRAPPER_DS = "org.jboss.resource.adapter.jdbc.WrapperDataSource"; //$NON-NLS-1$
	private static final String WRAPPER_DS_NAMEPREFIX = "java:"; //$NON-NLS-1$
	private static final String WRAPPER_DS_AS7 = "org.jboss.jca.adapters.jdbc.WrapperDataSource"; //$NON-NLS-1$
	private static final String WRAPPER_DS_NAMEPREFIX_AS7 = "java:/"; //$NON-NLS-1$
    private static final String JDBC_PREFIX = "java:/";
    private static final String TEIID_DRIVER_PREFIX = "teiid";
    
    private static final String UPDATE = "UPDATE";
    private static final String INSERT = "INSERT";
    private static final String DELETE = "DELETE";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String TABLE_SCHEM = "TABLE_SCHEM";
    private static final String PROCEDURE_NAME = "PROCEDURE_NAME";
    private static final String PROCEDURE_SCHEM = "PROCEDURE_SCHEM";
    private static final String SYS = "SYS";
    private static final String SYSADMIN = "SYSADMIN";
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String COLUMN_TYPE = "COLUMN_TYPE";
    private static final String TYPE_NAME = "TYPE_NAME";
    
	private Map<String,DataSource> mDatasources = new TreeMap<String,DataSource>();
	private Map<String,String> mDatasourceSchemas = new TreeMap<String,String>();

	private InitialContext context;

	/*
	 * Get List of all available Datasource Names.  This will refresh the Map of datasources, then
	 * return the datasource names.
	 * @param teiidOnly 'true' if only Teiid sources are to be returned, 'false' otherwise.
	 * @return the array of datasource names
	 */
	public String[] getAllDataSourceNames(boolean teiidOnly) {
		// Refresh the Map of Sources
		refreshDataSourceMap();
        
		// Get DataSource names
		List<String> resultList = new ArrayList<String>();
		
		Set<String> dsNames = mDatasources.keySet();
		Iterator<String> nameIter = dsNames.iterator();
		while(nameIter.hasNext()) {
			String dsName = nameIter.next();
			if(dsName!=null && !dsName.startsWith("java:/PREVIEW_")) {
				DataSource ds = mDatasources.get(dsName);
				if(!teiidOnly) {
					resultList.add(dsName);
				} else if(isTeiidSource(ds)) {
					resultList.add(dsName);
				}
			}
		}
		String[] resArray = new String[resultList.size()];
		int i=0;
		for(String name: resultList) {
			resArray[i] = name;
			i++;
		}
		return resArray;
	}
	
	/*
	 * @see org.teiid.tools.webquery.client.TeiidService#getTableAndColMap(java.lang.String)
	 */
	public Map<String,List<String>> getTableAndColMap(String dataSource) {
		
		// Get a connection for the supplied data source name
		Connection connection = getConnection(dataSource);
		
		// Result map of TableName with it's associated ColumnNames
		Map<String,List<String>> resultMap = new HashMap<String,List<String>>();

		boolean noTablesOrProcs = false;
		String[] tables = null;
		String[] procs = null;
		// Get Tables and Procedures for the Datasource
		if(connection==null || (dataSource!=null && dataSource.equalsIgnoreCase("NO SOURCES"))) {
			noTablesOrProcs = true;
		} else {
			// Get Tables and Procs for the Datasource
			tables = getTables(connection);
			procs = getProcedures(connection);
			
			// Determine if zero tables and procs
			if(tables.length==0 && procs.length==0) {
				noTablesOrProcs = true;
			}
		}

		// If there are zero tables and procedures, we can return here
		if(noTablesOrProcs) {
			// Return resultMap with "No Tables or Procs" designator
			resultMap.put("NO TABLES OR PROCS|TABLE", new ArrayList<String>());
			
			// Close Connection
			closeConnection(connection);
			
			return resultMap;
		}
		
		// Get Columns for each table.  Put entry in the Map
		for(int i=0; i<tables.length; i++) {
			String[] cols = getColumnsForTable(connection,tables[i]);
			List<String> colList = Collections.emptyList();
			if(cols!=null) {
				colList = Arrays.asList(cols);
			}
			// Add 'TABLE' designator onto Map Key
			resultMap.put(tables[i]+"|TABLE", colList);
		}
		
		// Get Columns for each procedure.  Put entry in the Map
		for(int i=0; i<procs.length; i++) {
			String[] cols = getColumnsForProcedure(connection,procs[i]);
			List<String> colList = Collections.emptyList();
			if(cols!=null) {
				colList = Arrays.asList(cols);
			}
			// Add 'PROC' designator onto Map Key
			resultMap.put(procs[i]+"|PROC", colList);
		}
		
		// Close the connection when finished
		if(connection!=null) {
			try {
				connection.close();
			} catch (SQLException e2) {

			}
		}
		
		return resultMap;
	}

	/*
	 * (non-Javadoc)
	 * @see org.teiid.tools.webquery.client.TeiidService#executeSql(java.lang.String, java.lang.String)
	 */
	public List<List<DataItem>> executeSql(String dataSource, String sql) throws SQLProcException {
		List<List<DataItem>> rowList = new ArrayList<List<DataItem>>();
		
		// Get connection for the datasource.  create a SQL Statement to issue the query.
		Connection connection = null;
		try {
			connection = getConnection(dataSource);
			if(connection!=null && sql!=null && sql.trim().length()>0) {
				sql = sql.trim();
				Statement stmt = connection.createStatement();
                String sqlUpperCase = sql.toUpperCase();
                
				// INSERT / UPDATE / DELETE - execute as an Update 
				if(sqlUpperCase.startsWith(INSERT) || sqlUpperCase.startsWith(UPDATE) || sqlUpperCase.startsWith(DELETE)) {
					int rowCount = stmt.executeUpdate(sql);
					List<DataItem> resultRow = new ArrayList<DataItem>();
					resultRow.add(new DataItem(rowCount+" Rows Updated",""));
					rowList.add(resultRow);	
				// SELECT
				} else {
					ResultSet resultSet = stmt.executeQuery(sql);

					int columnCount = resultSet.getMetaData().getColumnCount();
					List<DataItem> columnNames = new ArrayList<DataItem>();
					for (int i=1 ; i<=columnCount ; ++i) {
						columnNames.add(new DataItem(resultSet.getMetaData().getColumnName(i),"string"));
					}

					rowList.add(columnNames);

					if (!resultSet.isAfterLast()) {
						while (resultSet.next()) {
							List<DataItem> rowData = new ArrayList<DataItem>(columnCount);
							for (int i=1 ; i<=columnCount ; ++i) {
								rowData.add(createDataItem(resultSet,i));
							}
							rowList.add(rowData);
						}
					}
					resultSet.close();
				}
				stmt.close();
			}
		} catch (Exception e) {
			if(connection!=null) {
				try {
					connection.rollback();
				} catch (SQLException e2) {

				}
			}
			throw new SQLProcException(e.getMessage());
		} finally {
			if(connection!=null) {
				try {
					connection.close();
				} catch (SQLException e2) {

				}
			}
		}
		return rowList;
	}
	
	/*
	 * Determine if the data source is a Teiid source
	 * @param dataSource the data source
	 * @return 'true' if the source is a Teiid source
	 */
	private boolean isTeiidSource(DataSource dataSource) {
		boolean isVdb = false;
		Connection conn = null;
		if(dataSource!=null) {
			try {
				conn = dataSource.getConnection();
				if(conn!=null) {
					String driverName = conn.getMetaData().getDriverName();
					if(driverName!=null && driverName.trim().toLowerCase().startsWith(TEIID_DRIVER_PREFIX)) {
						isVdb = true;
					}
				}
			} catch (SQLException e) {
			} finally {
				if(conn!=null) {
					try {
						conn.close();
					} catch (SQLException e) {
					}
				}
			}
		}
		return isVdb;
	}

	/*
	 * Get List of Tables using the supplied connection
	 * @param connection the JDBC connection
	 * @return the array of table names
	 */
	private String[] getTables(Connection connection) {
		// Get the list of Tables
		List<String> tableNameList = new ArrayList<String>();
		List<String> tableSchemaList = new ArrayList<String>();
		if(connection!=null) {
			try {
				ResultSet resultSet = connection.getMetaData().getTables(null, null, "%", new String[]{"DOCUMENT", "TABLE", "VIEW"});
				int columnCount = resultSet.getMetaData().getColumnCount();
				while (resultSet.next()) {
					String tableName = null;
					String tableSchema = null;
					for (int i=1 ; i<=columnCount ; ++i) {
						String colName = resultSet.getMetaData().getColumnName(i);
						String value = resultSet.getString(i);
						if (colName.equalsIgnoreCase(TABLE_NAME)) {
							tableName = value;
						} else if(colName.equalsIgnoreCase(TABLE_SCHEM)) {
							tableSchema = value;
						}
					}
					tableNameList.add(tableName);
					tableSchemaList.add(tableSchema);
				}
				resultSet.close();
			} catch (Exception e) {
				if(connection!=null) {
					try {
						connection.rollback();
					} catch (SQLException e2) {

					}
				}
			} 
		}
		
		// Build full names if schemaName is present
		String[] tableNames = new String[tableNameList.size()];
		for(int i=0; i<tableNameList.size(); i++) {
			String schemaName = tableSchemaList.get(i);
			if(schemaName!=null && schemaName.length()>0) {
				tableNames[i]=schemaName+"."+tableNameList.get(i);
			} else {
				tableNames[i]=tableNameList.get(i);
			}
		}
		return tableNames;
	}

	/*
	 * Get List of Procedures using the supplied connection
	 * @param connection the JDBC connection
	 * @return the array of procedure names
	 */
	private String[] getProcedures(Connection connection) {
		// Get the list of Procedures
		List<String> procNameList = new ArrayList<String>();
		List<String> procSchemaList = new ArrayList<String>();
		if(connection!=null) {
			try {
				ResultSet resultSet = connection.getMetaData().getProcedures(null, null, "%");
				int columnCount = resultSet.getMetaData().getColumnCount();
				while (resultSet.next()) {
					String procName = null;
					String procSchema = null;
					for (int i=1 ; i<=columnCount ; ++i) {
						String colName = resultSet.getMetaData().getColumnName(i);
						String value = resultSet.getString(i);
						if (colName.equalsIgnoreCase(PROCEDURE_NAME)) {
							procName = value;
						} else if(colName.equalsIgnoreCase(PROCEDURE_SCHEM)) {
							procSchema = value;
						}
					}
					if(procSchema!=null && !procSchema.equalsIgnoreCase(SYS) && !procSchema.equalsIgnoreCase(SYSADMIN)) {
						procNameList.add(procName);
						procSchemaList.add(procSchema);
					}
				}
				resultSet.close();
			} catch (Exception e) {
				if(connection!=null) {
					try {
						connection.rollback();
					} catch (SQLException e2) {

					}
				}
			} 
		}
		
		// Build full names if schemaName is present
		String[] procNames = new String[procNameList.size()];
		for(int i=0; i<procNameList.size(); i++) {
			String schemaName = procSchemaList.get(i);
			if(schemaName!=null && schemaName.length()>0) {
				procNames[i]=schemaName+"."+procNameList.get(i);
			} else {
				procNames[i]=procNameList.get(i);
			}
		}
		return procNames;
	}

	/*
	 * Get List of Column names using the supplied connection and table name
	 * @param connection the JDBC connection
	 * @param fullTableName the Table name to get columns
	 * @return the array of Column names
	 */
	private String[] getColumnsForTable(Connection connection,String fullTableName) {
		if(connection==null || fullTableName==null || fullTableName.trim().isEmpty()) {
			return null;
		}
		List<String> columnNameList = new ArrayList<String>();
		List<String> columnTypeList = new ArrayList<String>();
		String schemaName = null;
		String tableName = null;
		int indx = fullTableName.lastIndexOf(".");
		if(indx!=-1) {
			schemaName = fullTableName.substring(0, indx);
			tableName = fullTableName.substring(indx+1);
		} else {
			tableName = fullTableName;
		}
		
		// Get the column name and type for the supplied schema and tableName
		try {
			ResultSet resultSet = connection.getMetaData().getColumns(null, schemaName, tableName, null);
			while(resultSet.next()) {
				String columnName = resultSet.getString(COLUMN_NAME);
				String columnType = resultSet.getString(TYPE_NAME);
				columnNameList.add(columnName);
				columnTypeList.add(columnType);
			}
			resultSet.close();
		} catch (Exception e) {
			if(connection!=null) {
				try {
					connection.rollback();
				} catch (SQLException e2) {

				}
			}
		} 
		
		// Pass back a delimited name|type result string
		String[] columns = new String[columnNameList.size()];
		for(int i=0; i<columnNameList.size(); i++) {
			String columnName = columnNameList.get(i);
			String columnType = columnTypeList.get(i);
			if(columnType!=null && columnType.length()>0) {
				columns[i]=columnName+"|"+columnType;
			} else {
				columns[i]=columnName;
			}
		}
		return columns;
	}

	/*
	 * Get List of Column names using the supplied connection and procedure name
	 * @param connection the JDBC connection
	 * @param fullProcName the Procedure name to get columns
	 * @return the array of Column names
	 */
	private String[] getColumnsForProcedure(Connection connection,String fullProcName) {
		if(connection==null || fullProcName==null || fullProcName.trim().isEmpty()) {
			return null;
		}
		List<String> columnNameList = new ArrayList<String>();
		List<String> columnDataTypeList = new ArrayList<String>();
		List<String> columnDirTypeList = new ArrayList<String>();
		String schemaName = null;
		String procName = null;
		int indx = fullProcName.lastIndexOf(".");
		if(indx!=-1) {
			schemaName = fullProcName.substring(0, indx);
			procName = fullProcName.substring(indx+1);
		} else {
			procName = fullProcName;
		}
		
		// Get the column name and type for the supplied schema and procName
		try {
			ResultSet resultSet = connection.getMetaData().getProcedureColumns(null, schemaName, procName, null);
			while(resultSet.next()) {
				String columnName = resultSet.getString(COLUMN_NAME);
				String columnType = resultSet.getString(COLUMN_TYPE);
				String columnDataType = resultSet.getString(TYPE_NAME);
				columnNameList.add(columnName);
				columnDataTypeList.add(columnDataType);
				columnDirTypeList.add(getProcColumnDirType(columnType));
			}
			resultSet.close();
		} catch (Exception e) {
			if(connection!=null) {
				try {
					connection.rollback();
				} catch (SQLException e2) {

				}
			}
		} 
		
		// Pass back a delimited name|type result string
		String[] columns = new String[columnNameList.size()];
		for(int i=0; i<columnNameList.size(); i++) {
			String columnName = columnNameList.get(i);
			String columnDataType = columnDataTypeList.get(i);
			if(columnDataType!=null && columnDataType.length()>0) {
				columns[i]=columnName+"|"+columnDataType;
			} else {
				columns[i]=columnName;
			}
		}
		return columns;
	}
	
	/*
	 * Interprets the procedure column type codes from jdbc call to strings
	 * @intStr the stringified code
	 * @return the direction type
	 */
	private String getProcColumnDirType(String intStr) {
		String result = "UNKNOWN";
		if(intStr!=null) {
			if(intStr.trim().equals("1")) {
				result = "IN";
			} else if(intStr.trim().equals("2")) {
				result = "INOUT";
			} else if(intStr.trim().equals("4")) {
				result = "OUT";
			} else if(intStr.trim().equals("3")) {
				result = "RETURN";
			} else if(intStr.trim().equals("5")) {
				result = "RESULT";
			} 		
		}
		return result;
	}

	/*
	 * Create a DataItem to pass back to client for each result
	 * @param resultSet the SQL ResultSet
	 * @param index the ResultSet index for the object
	 * @return the DataItem result
	 */
	private DataItem createDataItem(ResultSet resultSet, int index) throws SQLException {
		DataItem resultItem = null;
		String type = "string";
		Object obj = resultSet.getObject(index);
		
		String className = null;
		if(obj!=null) {
			className = obj.getClass().getName();
			if(className.equals("org.teiid.core.types.SQLXMLImpl")) {
				type = "xml";
			}
		}
		
		if(obj instanceof javax.sql.rowset.serial.SerialBlob) {
			byte[] bytes = ((SerialBlob)obj).getBytes(1, 500);
			resultItem = new DataItem(Arrays.toString(bytes),type);
		} else {
			String value = resultSet.getString(index);
			resultItem = new DataItem(value,type);
		}
		return resultItem;
	}
	
	/*
	 * Get Connection for the specified DataSource Name from the Map of DataSources
	 */
	private Connection getConnection (String datasourceName) {
		Connection connection = null;
		if(mDatasources!=null) {
			DataSource dataSource = (DataSource) mDatasources.get(datasourceName);
			if(dataSource!=null) {
				try {
					connection = dataSource.getConnection();
				} catch (SQLException e) {
				}
			}
		}
		return connection;
	}
	
	/*
	 * Close the supplied connection
	 */
	private void closeConnection(Connection conn) {
		if(conn!=null) {
			try {
				conn.close();
			} catch (SQLException e) {
			}
		}
	}
	
	/*
	 * Refresh the DataSource Maps
	 */
	private void refreshDataSourceMap( ) {
		// Clear the DataSource Maps
		mDatasources.clear();
		mDatasourceSchemas.clear();

		// New Context
		if(context==null) {
			try {
				context = new InitialContext();
			} catch (Exception e) {
			}
		}
		
		// Lookup all available JDBC Sources
		String prefix = JDBC_PREFIX;
		if(context!=null) {
			// Repopulate the maps
			NamingEnumeration<javax.naming.NameClassPair> ne = null;
			try {
				Context jdbcContext = (Context) context.lookup(JDBC_PREFIX);
				ne = jdbcContext.list("");
			} catch (NamingException e1) {
			}
			while (ne!=null && ne.hasMoreElements()) {
				javax.naming.NameClassPair o = (javax.naming.NameClassPair) ne.nextElement();
				Object bindingObject = null;

				try {
					if (o.getClassName().equals(WRAPPER_DS)) {
						bindingObject = context.lookup(WRAPPER_DS_NAMEPREFIX + o.getName());
					} else if(o.getClassName().equals(WRAPPER_DS_AS7)) {
						bindingObject = context.lookup(WRAPPER_DS_NAMEPREFIX_AS7 + o.getName());
					}
				} catch (NamingException e1) {
					System.out.println("Error with lookup of "+o.getName());
				}
				
				if(bindingObject!=null && bindingObject instanceof DataSource) {
					// Put DataSource into datasource Map
					String key = prefix.concat(o.getName());
					mDatasources.put(key, (DataSource)bindingObject);

					// Put Schema into schema Map
					String schema = null;
					try {
						schema = (String) context.lookup("java:comp/env/schema/" + key);
					} catch (NamingException e) {

					}
					mDatasourceSchemas.put(key, schema);
				}
			}
		}
	}

}
