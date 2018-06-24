package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Helper class used to store and retrieve access history information about tags processed by the extractor.
 * Provides functions to create and delete the history table,
 */
public class HistoryManager {
	
	public static final String POOL_SIZE = "64";
	
	public static final String OFFSET_DEFAULT_TABLENAME = "OFFSET_TABLE";
	public static final byte[] OFFSET_DEFAULT_FAMILY = Bytes.toBytes("F");
	public static final byte[] WEBID_QUALIFIER = Bytes.toBytes("WebID");	
	public static final byte[] TIMESTAMP_Q = Bytes.toBytes("Timestamp");

//	private Table dataTable;
	private Table offsetTable;
	private Connection connection;
	private Admin admin;

	/**
	 * Constructor for the HBase history manager.
	 * @throws IOException
	 */
	public HistoryManager() throws IOException {
		Configuration config = HBaseConfiguration.create();
		/** HBase config is done through the hbase-site.xml file.
		 *  The main settings that must be configured are:
		 * 		- hbase.master
		 * 		- hbase.zookeeper.quorum
		 * 		- hbase.zookeeper.property.clientPort
		 */
		config.addResource(new Path(this.getClass().getClassLoader().getResource("hbase-site.xml").getPath()));
		connection = ConnectionFactory.createConnection(config);
		admin = connection.getAdmin();
		offsetTable = connection.getTable(TableName.valueOf(OFFSET_DEFAULT_TABLENAME));
//		reset();
	}

	/**
	 * Retrieves the history map from HBase.
	 * @return
	 * @throws IOException
	 */
	public Map<String,HistoryEntry> getHistoryMap() throws IOException {
		Scan scan = new Scan();
		scan.setCaching(100);
		scan.setMaxVersions(1); //required?
		ResultScanner rScanner = offsetTable.getScanner(new Scan());
		Map<String, HistoryEntry> map = new ConcurrentHashMap<>(100);
		// [CMS]
		System.out.println("\n\t[TAG] \t\t\t[Timestamp HBase]");		
		rScanner.forEach(result -> {
			try {
				Result row = offsetTable.get(new Get(result.getRow()));
				String id = Bytes.toString(row.getValue(OFFSET_DEFAULT_FAMILY, WEBID_QUALIFIER));
				String ts = Bytes.toString(row.getValue(OFFSET_DEFAULT_FAMILY, TIMESTAMP_Q));
				String key = Bytes.toString(row.getRow());
				HistoryEntry entry = new HistoryEntry(id, ts);		
				map.put(key, entry);	
				// [CMS]
				System.out.println("\t" + key + "\t " + ts);	
			} catch (IOException e) {
				e.printStackTrace();
			}		
		});	
		return map;
	}

	/**
	 * Writes the map of extracted data points to HBase.
	 * @param history
	 * @throws IOException
	 */
	public void writeHistory(Map<String, HistoryEntry> history) throws IOException {
		List<Put> puts = new ArrayList<Put>(history.size());
		// [CMS]
		System.out.println("\nAdding new entries... ");
		System.out.println("\n\t[TAG] \t\t\t[Timestamp HBase]");	
		for(String key : history.keySet()) {
			HistoryEntry entry = history.get(key);
			byte[] row = Bytes.toBytes(key);
			byte[] webID = Bytes.toBytes(entry.webID());
			byte[] timestamp = Bytes.toBytes(entry.timestamp());
			Put put = new Put(row);
			put.addColumn(OFFSET_DEFAULT_FAMILY, WEBID_QUALIFIER, webID);
			put.addColumn(OFFSET_DEFAULT_FAMILY, TIMESTAMP_Q, timestamp);
			puts.add(put);
			// [CMS]	
			System.out.println("\t" + key + "\t " + entry.timestamp());
		}
		offsetTable.put(puts);
	}

	/////////////////////////////////////////////   NOT USED   ////////////////////////////////////////////////////	
	
	/**
	 * Creates a new table in HBase.
	 * @param table - Table name
	 * @param families - List of column families
	 * @throws IOException
	 */
	/*private void createTable(String table, List<String> families) throws IOException {
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
		for(String family : families) 
			desc.addFamily(new HColumnDescriptor(family));

		admin.createTable(desc);
	}

	/**
	 * Deletes a table from HBase.
	 * @param table - Table to delete
	 * @throws IOException
	 */
	/*
	private void deleteTable(String table) throws IOException {
		TableName tbl = TableName.valueOf(table);
		if (admin.tableExists(tbl)) {
			admin.disableTable(tbl);
			admin.deleteTable(tbl);
		}
	}

	/**
	 * Drops and recreates the history table.
	 * Should really not be used outside of testing, as it means all tags will have to be extracted from scratch.
	 * Just delete tags from history manually.
	 * @throws IOException
	 */
	/*
	public void reset() throws IOException {
		deleteTable(OFFSET_DEFAULT_TABLENAME);
		createTable(OFFSET_DEFAULT_TABLENAME,Arrays.asList(Bytes.toString(OFFSET_DEFAULT_FAMILY)));
		System.out.println("Wiped history.");
	}
	
	//TODO these were meant to send data to HBase, but that gets handled elsewhere
	private void get(String row) throws IOException {
		Get get = new Get(Bytes.toBytes(row));
		Result result = dataTable.get(get);
		byte[] value = result.value();
		System.out.println("data: " + Bytes.toString(value));
	}

	private void add(String row, String family, String qualifier, byte[] value) throws IOException {
		Put put = new Put(Bytes.toBytes(row));
		put.addColumn(family.getBytes(), qualifier.getBytes(), value);
		dataTable.put(put);
	}

	private void delete(String row, String family, String qualifier) throws IOException {
		Delete delete = new Delete(Bytes.toBytes(row));
		delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		dataTable.delete(delete);
	}
	*/
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////	
}

