package org.teiid.tools.webquery.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Document;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.DomEvent;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.DialogBox;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.ScrollPanel;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.VerticalPanel;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 */
public class webquery implements EntryPoint {
	/**
	 * The message displayed to the user when the server cannot be reached or
	 * returns an error.
	 */
	private static final String SERVER_ERROR = "An error occurred while "
		+ "attempting to contact the server. Please check your network "
		+ "connection and try again.";
	private static final String DATASOURCE_REFRESH_ERROR = "An error occurred while "
		+ "attempting to refresh the datasources.";
	private static final String TABLE_REFRESH_ERROR = "An error occurred while "
		+ "attempting to refresh the datasource tables.";

	/**
	 * Create a remote service proxy to talk to the server-side Teiid service.
	 */
	private final TeiidServiceAsync teiidService = GWT.create(TeiidService.class);

	private final Messages messages = GWT.create(Messages.class);

	private TextArea sqlTextArea = new TextArea();
	private FlexTable resultsTable = new FlexTable();
	private ListBox datasourceListBox = new ListBox();
	private ListBox tablesListBox = new ListBox();
	private CheckBox onlyVdbsCheckBox = new CheckBox();
	private Button refreshButton = new Button();
	private Button submitButton = new Button();
	//private Button cancelButton = new Button();
	private Label titleLabel = new Label();
	private Label datasourcesLabel = new Label();
	private Label tablesLabel = new Label();
	private Label sqlAreaLabel = new Label();
	private Label resultsAreaLabel = new Label();
	private Label tablesLoadStatusLabel = new Label();
	
	private FlexTable columnsTable = new FlexTable();
	private Label columnsAreaLabel = new Label();

	private DialogBox errorDialogBox = new DialogBox();
	private Button errorDialogCloseButton = new Button("Close");
	private HTML serverResponseLabel = new HTML();
	private DialogBox xmlDialogBox = new DialogBox();
	private Button xmlDialogCloseButton = new Button("Close");
	private TextArea xmlDataTextArea = new TextArea();
//	private Image currentImage;
//	private Image blankImage = new Image("./OpenShiftBlank.png");
//	private Image portfolioImage = new Image("./PortfolioVDB.png");
//	private Image mySQLImage = new Image("./MySQLDB.png");
//	private Image salesforceImage = new Image("./SalesforceVDB.png");
	
	private Map<String,List<String>> tableColMap = new HashMap<String,List<String>>();
	private Map<String,List<String>> procColMap = new HashMap<String,List<String>>();
	private Map<String,String> tableTypeMap = new HashMap<String,String>();
	
	/**
	 * This is the entry point method.
	 */
	public void onModuleLoad() {

		// ---------------------------------------
		// Set SQL Controls area widgets
		// ---------------------------------------

		// Title Label
		titleLabel.setText("Web Query - JDBC");
		titleLabel.addStyleName("applicationTitle");
		RootPanel.get("titleLabelContainer").add(titleLabel);

		// Datasources, Tables, Procedures :  ListBox Labels
		datasourcesLabel.setText("Datasources:");
		datasourcesLabel.addStyleName("headerLabel");
		RootPanel.get("datasourcesLabelContainer").add(datasourcesLabel);
		tablesLabel.setText("Tables & Procedures:");
		tablesLabel.addStyleName("headerLabel");
		RootPanel.get("tablesLabelContainer").add(tablesLabel);

		// Datasources, Tables, Procedures :  ListBoxes
		RootPanel.get("datasourcesListBoxContainer").add(datasourceListBox);
		RootPanel.get("tablesListBoxContainer").add(tablesListBox);
		tablesLoadStatusLabel.addStyleName("tablesLoadStatusLabel");
		RootPanel.get("tableLoadStatusLabelContainer").add(tablesLoadStatusLabel);

	    onlyVdbsCheckBox.setText("Teiid VDBs Only");
	    onlyVdbsCheckBox.setValue(false);
	    RootPanel.get("datasourcesFilterCheckBoxContainer").add(onlyVdbsCheckBox);
	    
	    refreshButton.setText("Refresh");
		// Click Handlers for reload button
	    refreshButton.addClickHandler(new ClickHandler() {
			public void onClick(ClickEvent event) {
				refreshDatasourceListBox();
			}
		});
	    RootPanel.get("refreshButtonContainer").add(refreshButton);
	    
		// SQL Area Label
		sqlAreaLabel.setText("SQL Query:");
		sqlAreaLabel.addStyleName("headerLabel");
		RootPanel.get("sqlLabelContainer").add(sqlAreaLabel);

		// SQL Text Area
		sqlTextArea.setCharacterWidth(80);
		sqlTextArea.setHeight("150px");
		RootPanel.get("sqlTextContainer").add(sqlTextArea);

		// Submit and Cancel Buttons
		submitButton.setText("Submit");
		//cancelButton.setText("Cancel");
		// Click Handlers for Submit and Cancel
		submitButton.addClickHandler(new ClickHandler() {
			public void onClick(ClickEvent event) {
				doSubmit();
			}
		});
		RootPanel.get("submitButtonContainer").add(submitButton);

		// ---------------------------------------
		// Set Column Table widgets
		// ---------------------------------------

		// Title Label
		columnsAreaLabel.setText("Available Columns");
		columnsAreaLabel.addStyleName("headerLabel");
		RootPanel.get("columnsLabelContainer").add(columnsAreaLabel);
		
		// Results Table style
		columnsTable.addStyleName("resultsTable");
        ScrollPanel colScroll = new ScrollPanel(columnsTable);
        colScroll.setHeight("150px");
		RootPanel.get("columnsTableContainer").add(colScroll);

		// Graphic on RH upper panel
//		currentImage = blankImage;
//		RootPanel.get("imageContainer").add(currentImage);

		// ---------------------------------------
		// Set Results area widgets
		// ---------------------------------------

		// Title Label
		resultsAreaLabel.setText("Query Results - 0 Rows");
		resultsAreaLabel.addStyleName("resultsPanelTitle");
		RootPanel.get("resultsLabelContainer").add(resultsAreaLabel);
		
		// Results Table style
		resultsTable.addStyleName("resultsTable");

		ScrollPanel resultsScroll = new ScrollPanel(resultsTable);
		resultsScroll.setHeight("350px");
		resultsScroll.setWidth("1300px");
		RootPanel.get("resultsTableContainer").add(resultsScroll);

		// Init Dialogs for later use.
		initErrorDialog();
		initXmlDialog();

		// ---------------------------------------
		// Handlers for ListBox Controls
		// ---------------------------------------

		// Change Listener for DataSource dropdown    
		datasourceListBox.addChangeHandler(new ChangeHandler()
		{
			public void onChange(ChangeEvent event)
			{
				int selectedIndex = datasourceListBox.getSelectedIndex();
				String selectedDS = datasourceListBox.getValue(selectedIndex);
				refreshTablesListBox(selectedDS);
				//updateGraphic(selectedDS);
			}
		});

		// Change Handler for Tables dropdown
		tablesListBox.addChangeHandler(new ChangeHandler()
		{
			public void onChange(ChangeEvent event)
			{
				//submitButton.setEnabled(false);
				int selectedIndex = tablesListBox.getSelectedIndex();
				String selectedTable = tablesListBox.getValue(selectedIndex);
				refreshColumnsTable(selectedTable);
				refreshSQLTextArea();
				populateNoResultsToDisplay();
				submitButton.setEnabled(true);
			}
		});

//		sqlTextArea.addChangeHandler(new ChangeHandler()
//		{
//			public void onChange(ChangeEvent event)
//			{
//				populateNoResultsToDisplay();
//			}
//		});
		
	    // Hook up a handler to find out when it's clicked.
	    onlyVdbsCheckBox.addClickHandler(new ClickHandler() {
	      public void onClick(ClickEvent event) {
	    	  refreshDatasourceListBox();
	      }
	    });
		
		// Refresh the DataSource listBox.  This selects first item,
		// and fires event to refresh the tables box and SQL Area
		refreshDatasourceListBox();

	}
	
	/*
	 * Refresh the Datasources ListBox.  Populates the List with all
	 * available jdbc sources and selects the first item
	 */
	private void refreshDatasourceListBox() {
		// Set up the callback object.
		AsyncCallback<String[]> callback = new AsyncCallback<String[]>() {
			// On Failure - show Error Dialog
			public void onFailure(Throwable caught) {
				// Dialog Title
				errorDialogBox.setText("Datasource Refresh Error");
				serverResponseLabel.addStyleName("serverResponseLabelError");
				// Dialog Message
				serverResponseLabel.setHTML(DATASOURCE_REFRESH_ERROR);
				errorDialogBox.center();
				errorDialogCloseButton.setFocus(true);
			}

			// On Success - Populate the ListBox with Datasource Names
			public void onSuccess(String[] result) {
				List<String> datasourceList = Arrays.asList(result);
				Collections.sort(datasourceList);

				// Populate the Datasource ListBox
				if(datasourceListBox.getItemCount()>0) datasourceListBox.clear();
				int i=0;
				for(String longDSName: datasourceList) {
					datasourceListBox.insertItem(longDSName, i);
					i++;
				}
				// Add Placeholder if no sources
				if(datasourceListBox.getItemCount()==0) {
					datasourceListBox.insertItem("NO SOURCES", 0);
				} 
				datasourceListBox.setSelectedIndex(0);
				DomEvent.fireNativeEvent(Document.get().createChangeEvent(),datasourceListBox);				
			}
		};

		// Make the Remote Server call to init the ListBox
		teiidService.getAllDataSourceNames(onlyVdbsCheckBox.getValue().booleanValue(), callback);
	}

	/*
	 * Update the RH Upper panel Image, based on the selected datasource
	 */
//	private void updateGraphic(String datasourceName) {
//		RootPanel panel = RootPanel.get("imageContainer");
//		panel.remove(currentImage);
//		if(datasourceName.equals("java:/Accounts")) {
//			currentImage = mySQLImage;
//		} else if(datasourceName.equals("java:/PortfolioVDB")) {
//			currentImage = portfolioImage;
//		} else if(datasourceName.equals("java:/SalesforceVDB")) {
//			currentImage = salesforceImage;
//		} else {
//			currentImage = blankImage;
//		}
//		panel.add(currentImage);
//	}
	
	/*
	 * Refresh the Tables ListBox.  Populates the List with all tables for
	 * the supplied dataSource, and sets the SQL Area Text
	 */
	private void refreshTablesListBox(String datasourceName) {
		//submitButton.setEnabled(false);

		// Set up the callback object.
		AsyncCallback<Map<String,List<String>>> callback = new AsyncCallback<Map<String,List<String>>>() {
			// On Failure - show Error Dialog
			public void onFailure(Throwable caught) {
				tablesLoadStatusLabel.setText("");
				
				tableTypeMap.clear();
				// Dialog Title
				errorDialogBox.setText("Datasource Tables Refresh Error");
				serverResponseLabel.addStyleName("serverResponseLabelError");
				// Dialog Message
				serverResponseLabel.setHTML(TABLE_REFRESH_ERROR);
				errorDialogBox.center();
				errorDialogCloseButton.setFocus(true);
				submitButton.setEnabled(true);
			}

			// On Success - Populate the Table ListBox
			public void onSuccess(Map<String,List<String>> result) {				
				// Reset the Table-Column Map for later use.
				tableColMap = result;
				
				// Populate the tables ListBox
				//if(tablesListBox.getItemCount()>0) tablesListBox.clear();
				
				List<String> tableList = new ArrayList<String>();
				tableTypeMap.clear(); 
				// Remove the TABLE / PROC designators and keep track of type
				Set<String> tableProcKeys = tableColMap.keySet();
				Iterator<String> keyIter = tableProcKeys.iterator();
				while(keyIter.hasNext()) {
					String tableProcKey = keyIter.next();
					int vBarIndx = tableProcKey.indexOf('|');
					String name = tableProcKey.substring(0, vBarIndx);
					String type = tableProcKey.substring(vBarIndx+1);
					// Add Table - Proc Name to List
					tableList.add(name);
					// Keep track of whether it is a Table or Procedure
					tableTypeMap.put(name,type);
				}
				
				Collections.sort(tableList);
				
				int i=0;
				for(String tblName: tableList) {
					tablesListBox.insertItem(tblName, i);
					i++;
				}
				// Init selection to first value
				if(tablesListBox.getItemCount()>0) tablesListBox.setSelectedIndex(0);
				
				// Refresh the Columns Table
				int selectedIndex = tablesListBox.getSelectedIndex();
				String selectedTable = tablesListBox.getValue(selectedIndex);
				refreshColumnsTable(selectedTable);
				
				// Refresh SQL Area
				refreshSQLTextArea();
				populateNoResultsToDisplay();
				
				submitButton.setEnabled(true);
				tablesLoadStatusLabel.setText("");
			}
		};

		// Prior to refresh, 
		//   - clear the available Tables ListBox
		//   - clear the available columns Table
		//   - Display 'Loading Tables...' message
		if(tablesListBox.getItemCount()>0) tablesListBox.clear();
		clearColumnsTable();
		tablesLoadStatusLabel.setText("Loading Tables...");

		// Make the remote server call.
		teiidService.getTableAndColMap(datasourceName, callback);
	}
	
	/*
	 * Refresh the Columns Table.  Get Column info from the tableColMap
	 * and repopulate the table.
	 */
	private void refreshColumnsTable(String tableName) {
		String type = tableTypeMap.get(tableName);
		String key = null;
		if(type!=null) {
			key = tableName + "|" + type;
		} else {
			key = tableName + "|TABLE";
		}

		// Results for the selected table
		List<String> resultList = tableColMap.get(key);

		// Create List of Table Rows
		List<List<String>> rowList = new ArrayList<List<String>>();

		// Add Header Row
		List<String> headerRow = new ArrayList<String>();
		headerRow.add("Column Name");
		headerRow.add("Column Type");
		rowList.add(headerRow);
		
		// Parse out column name and type, and add row.
		// The column name may not have an associated type.  If not, set the type to "unk"
		for(String nameTypeStr: resultList) {
			List<String> dataRow = new ArrayList<String>();
			// name and type are separated by a '|' - (may not have a type)
			int indx = nameTypeStr.indexOf("|");
			String colName = null;
			String colType = null;
			// Delimiter found - set the name and type
			if(indx!=-1) {
				colName = nameTypeStr.substring(0,indx);
				colType = nameTypeStr.substring(indx+1);
		    // Delimiter not found - use entire string for name & set type to 'unk'
			} else {
				colName = nameTypeStr;
				colType = "unknown";
			}
			dataRow.add(colName);
			dataRow.add(colType);
			rowList.add(dataRow);
		}
		
		// Populate the Table with the new rows
		populateColumnsTable(rowList);
	}

	/*
	 * Refresh the SQL Text area, using the current Tables
	 * ListBox item to generate "SELECT * FROM <Table>"
	 * the supplied dataSource, and sets the SQL Area Text
	 */
	private void refreshSQLTextArea( ) {
		// Get the Table.  If there is a selected table, use it.
		// Otherwise, get the first item in the list.
		String selectedTable = null;
		int tableIndex = tablesListBox.getSelectedIndex();
		if(tablesListBox.getSelectedIndex()>=0) {
			selectedTable = tablesListBox.getItemText(tableIndex);
		} else {
			selectedTable = tablesListBox.getItemText(0);
		}
		
		// Determine if the selection is a Table or Procedure
		String type = tableTypeMap.get(selectedTable);
		StringBuffer sb = new StringBuffer();
		if(type==null || type.equalsIgnoreCase("TABLE")) {
			sb.append("SELECT * FROM ");
			if(!selectedTable.equalsIgnoreCase("NO TABLES")) {
				sb.append(selectedTable);
			}
		} else if(type.equalsIgnoreCase("PROC")) {
			sb.append("SELECT * FROM (EXEC ");
			sb.append(selectedTable);
			sb.append("(<params>)) AS Result");
		}

		sqlTextArea.setText(sb.toString());
	}

	/*
	 * Handler for Submit Button Pressed
	 */
	private void doSubmit() {
		// Get the selected source
		int srcIndex = datasourceListBox.getSelectedIndex();
		String selectedSource = datasourceListBox.getValue(srcIndex);
		//submitButton.setEnabled(false);

		// Get SQL
		String sql = sqlTextArea.getText();

		// Set up the callback object.
		AsyncCallback<List<List<DataItem>>> callback = new AsyncCallback<List<List<DataItem>>>() {
			// On Failure - show Error Dialog
			public void onFailure(Throwable caught) {
				String msg = SERVER_ERROR;
				if(caught instanceof SQLProcException) {
					msg = ((SQLProcException)caught).getSqlDetail();
				}
				// Dialog Title
				errorDialogBox.setText("Query Submittal Error");
				serverResponseLabel.addStyleName("serverResponseLabelError");
				// Dialog Text
				serverResponseLabel.setHTML(msg);
				errorDialogBox.center();
				errorDialogCloseButton.setFocus(true);
				submitButton.setEnabled(true);
			}

			// On Success - Populate the ListBox
			public void onSuccess(List<List<DataItem>> rowData) {
				populateResultsTable(rowData);
				submitButton.setEnabled(true);
			}
		};

		teiidService.executeSql(selectedSource, sql, callback);
	}
	
	/*
	 * Clear the Results Table and re-populate it with the supplied data
	 */
	private void populateResultsTable(List<List<DataItem>> rowData) {
		// Clear Previous Results
		clearResultsTable();

		int iRow = 0;
		for(List<DataItem> row: rowData) {
			int nCols = row.size();
			for(int i=0; i<nCols; i++) {
				DataItem data = (DataItem)row.get(i);
				if(data.getType().contains("xml")) {
					final String xmlData = data.getData();
					// Show XML Button
					Button xmlButton = new Button();
					xmlButton.setText("Show XML");
					// Click Handlers for Submit and Cancel
					xmlButton.addClickHandler(new ClickHandler() {
						public void onClick(ClickEvent event) {
							// Dialog Title
							xmlDialogBox.setText("XML Result");
							// Dialog Message
							xmlDataTextArea.setText(xmlData);
							xmlDialogBox.center();
							xmlDialogCloseButton.setFocus(true);
						}
					});
					resultsTable.setWidget(iRow, i, xmlButton);
				} else {
					resultsTable.setText(iRow,i,data.getData());
				}
			}
			
			// Header Row Style
			if(iRow==0) {
				resultsTable.getRowFormatter().addStyleName(iRow, "resultsTableHeader");
		    // Even Row Style
			} else {
				boolean isEven = (iRow % 2 == 0);
				if(isEven) {
					resultsTable.getRowFormatter().addStyleName(iRow, "resultsTableEvenRow");
				} else {
					resultsTable.getRowFormatter().addStyleName(iRow, "resultsTableOddRow");
				}
			}
			iRow++;
		}
		resultsAreaLabel.setText("Query Results - " + (rowData.size()-1) +" Rows");
		
	}
	
	/*
	 * Clear the Results Table and re-populate it with the supplied data
	 */
	private void populateNoResultsToDisplay( ) {
		// Clear Previous Results
		clearResultsTable();

		// Simple message with no results
		resultsTable.setText(0,0,"No Results to Display");
		resultsTable.getRowFormatter().addStyleName(0, "resultsTableHeader");
		
		resultsAreaLabel.setText("Query Results - 0 Rows");
	}
	
	/*
	 * Clear the Results Table and re-populate it with the supplied data
	 */
	private void populateColumnsTable(List<List<String>> rowData) {
		// Clear Previous Results
		clearColumnsTable();

		int iRow = 0;
		for(List<String> row: rowData) {
			int nCols = row.size();
			for(int i=0; i<nCols; i++) {
				columnsTable.setText(iRow,i,row.get(i));
			}
			
			// Header Row Style
			if(iRow==0) {
				columnsTable.getRowFormatter().addStyleName(iRow, "resultsTableHeader");
		    // Even Row Style
			} else {
				boolean isEven = (iRow % 2 == 0);
				if(isEven) {
					columnsTable.getRowFormatter().addStyleName(iRow, "resultsTableEvenRow");
				} else {
					columnsTable.getRowFormatter().addStyleName(iRow, "resultsTableOddRow");
				}
			}
			iRow++;
		}
		
	}
	
	/*
	 * Clear the Results Table 
	 */
	private void clearColumnsTable( ) {
		columnsTable.removeAllRows();
	}

	/*
	 * Clear the Results Table 
	 */
	private void clearResultsTable( ) {
		resultsTable.removeAllRows();
	}

	/*
	 * Handler for Cancel Button Pressed
	 */
//	private void doCancel() {
//		Window.alert("Cancel Button Pressed");
//	}

	/*
	 * Init the Dialog for Error Display
	 */
	private void initErrorDialog() {
		// Create the popup Error DialogBox
		errorDialogBox.setText("Error Dialog");
		errorDialogBox.setAnimationEnabled(true);
		// We can set the id of a widget by accessing its Element
		errorDialogCloseButton.getElement().setId("closeButton");
		VerticalPanel dialogVPanel = new VerticalPanel();
		dialogVPanel.setHeight("100px");
		dialogVPanel.addStyleName("dialogVPanel");
		dialogVPanel.add(serverResponseLabel);
		dialogVPanel.setHorizontalAlignment(VerticalPanel.ALIGN_RIGHT);
		dialogVPanel.add(errorDialogCloseButton);
		errorDialogBox.setWidget(dialogVPanel);

		// Add a handler to close the DialogBox
		errorDialogCloseButton.addClickHandler(new ClickHandler() {
			public void onClick(ClickEvent event) {
				errorDialogBox.hide();
			}
		});
	}
	
	/*
	 * Init the Dialog for Error Display
	 */
	private void initXmlDialog() {
		// Create the popup Error DialogBox
		xmlDialogBox.setText("XML Result");
		//xmlDialogBox.setAnimationEnabled(true);
		// We can set the id of a widget by accessing its Element
		xmlDialogCloseButton.getElement().setId("closeButton");
		VerticalPanel dialogVPanel = new VerticalPanel();
		dialogVPanel.setHeight("200px");
		dialogVPanel.setWidth("400px");
		dialogVPanel.addStyleName("dialogVPanel");
		xmlDataTextArea.setCharacterWidth(45);
		xmlDataTextArea.setHeight("200px");

		dialogVPanel.add(xmlDataTextArea);
		dialogVPanel.setHorizontalAlignment(VerticalPanel.ALIGN_RIGHT);
		dialogVPanel.add(xmlDialogCloseButton);
		xmlDialogBox.setWidget(dialogVPanel);
		
		// Add a handler to close the DialogBox
		xmlDialogCloseButton.addClickHandler(new ClickHandler() {
			public void onClick(ClickEvent event) {
				xmlDialogBox.hide();
			}
		});
	}
		
//	public static String prettyFormat(String input, int indent) {
//	    try {
//	        Source xmlInput = new StreamSource(new StringReader(input));
//	        StringWriter stringWriter = new StringWriter();
//	        StreamResult xmlOutput = new StreamResult(stringWriter);
//	        TransformerFactory transformerFactory = TransformerFactory.newInstance();
//	        transformerFactory.setAttribute("indent-number", indent);
//	        Transformer transformer = transformerFactory.newTransformer(); 
//	        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
//	        transformer.transform(xmlInput, xmlOutput);
//	        return xmlOutput.getWriter().toString();
//	    } catch (Exception e) {
//	        throw new RuntimeException(e); // simple exception handling, please review it
//	    }
//	}
}
