package extractor;

/**
 * Represents an entry in the .csv file used to add tags to the extraction system.
 */
public class CSVEntry {
	private String tag;
	private String server;
	private String timestamp; 
	
	public CSVEntry(String server, String tag, String timestamp) {
		this.server = server;
		this.tag = tag;
		this.timestamp = timestamp;
	}

	public String server() {
		return this.server;
	}

	public String tag() {
		return this.tag;
	}

	public String timestamp() {
		return this.timestamp;
	}
}
