package hbase;

/**
 * Represents an entry in the history table used by the program to keep track of extracted data points.
 * Contains the webID and last processed timestamp of the tag it refers to.
 * @author andre
 *
 */
public class HistoryEntry {
	private String timestamp;
	private String webID;
	
	public HistoryEntry(String webID, String timestamp) {
		this.webID = webID;
		this.timestamp = timestamp;
	}

	public String webID() {
		return this.webID;
	}

	public String timestamp() {
		return this.timestamp;
	}
}
