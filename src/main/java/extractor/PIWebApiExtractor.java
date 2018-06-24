package extractor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketAdapter;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketFactory;
import com.neovisionaries.ws.client.WebSocketFrame;
import com.osisoft.pidevclub.piwebapi.ApiException;
import com.osisoft.pidevclub.piwebapi.PIWebApiClient;
import com.osisoft.pidevclub.piwebapi.models.PIDataServer;
import com.osisoft.pidevclub.piwebapi.models.PIPoint;
import com.osisoft.pidevclub.piwebapi.models.PIStreamValues;
import com.osisoft.pidevclub.piwebapi.models.PITimedValue;

import hbase.HistoryEntry;
import producer.KafkaPIDataProducer;

/**
 *	Extractor class for the PI Web API
 * 	Extracted data is serialized into byte arrays and sent to Kafka.
 */
public final class PIWebApiExtractor {
	
	/* API Acess Defaults */
	static final String API_URL = "op.skipper.corp.edp.pt/piwebapi";
	static final String DEFAULT_DATA_TOPIC = "csvpt_bempostaII_1BAC";
	
	public static final String PI_PATH_PREFIX_SERVER = "\\\\";
	public static final String PI_PATH_PREFIX_TAG = "\\";
	
	public static final int INTERPOLATED_DATA = 1;
	public static final int RAW_DATA = 0;
	
	private final PIWebApiClient client;
	private final KafkaPIDataProducer producer;
	private final ObjectMapper mapper;
	
	/* Parameters */
	private static String startTime; 
	private static String endTime;
	private static int maxRecords; 
	private static int maxPIPoints;
	// Raw, Interpolated
	private static int dataType;
	//Required for the update listener..
	private String username;
	private String password;
	
	//TODO map size
	private static Map<String, HistoryEntry> history = new ConcurrentHashMap<>(100);
	
	/**
	 * Constructor for the PIWebApiExtractor.
	 * Performs an authentication check to ensure the client is set up properly, by calling .getHome()
	 * 
	 * @param username
	 * @param password
	 * @param kafkaProducer
	 * @throws ApiException
	 */
	public PIWebApiExtractor(String username, String password, KafkaPIDataProducer kafkaProducer,String start, String end, int nRecords, int nPoints, int type, boolean debug) throws ApiException {
	
		client = new PIWebApiClient("https://"+API_URL, username, password, false, debug);
		
		// Authentication Test
		client.getHome().get();

		this.username = username;
		this.password = password;
		
		startTime = start;
		endTime = end;
		maxRecords = nRecords;
		maxPIPoints = nPoints;
		dataType = type;
		
		producer = kafkaProducer;
		
		//Mapper to parse json into PITimedValues
		mapper = new ObjectMapper();
		mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true); 
	}

	/**
	 * Adds an entry to the history map.
	 * 
	 * @param key
	 * @param webID
	 * @param timestamp
	 */
	private void addToHistory(String key, String webID, String timestamp) {
		history.put(key, new HistoryEntry(webID, timestamp));
	}
	
	/**
	 * Returns the current history map containing tag process info.
	 * @return map of latest produced values for each tag
	 */
	public Map<String, HistoryEntry> getHistoryMap() {
		return history;
	}
	
	/**
	 * Sets the history map.
	 * @param map
	 */
	public void setHistoryMap(Map<String, HistoryEntry> map) {
		history = map;
	}
	
	/**
	 * Sends the values in a streamset to kafka.
	 * 
	 * @param streamset - A list of tags and their datasets
	 * @throws ApiException
	 * @throws IOException 
	 */
	public void produceStreamSet(String serverName, List<PIStreamValues> streamset) throws ApiException {
		if(streamset.isEmpty())
			return;
		
		//A streamvalue also represents a tag
		for(PIStreamValues sv : streamset) {
			String key = serverName+":"+sv.getName();
			produceToKafka(key, sv.getWebId(), sv.getItems());
		}
	}

	/**
	 * Sends each data point in the streamset to Kafka.
	 * @param  key			tag (name) a data point in the API
	 * @param  webID		webID of a data point in the API
	 * @param  timedValues	list of PITimedValues, each representing a point in the time series of a tag
	 */
	public void produceToKafka(String key, String webID, List<PITimedValue> timedValues) {
		if(!timedValues.isEmpty()) {		
			for(PITimedValue timedvalue : timedValues) {
				producer.send(DEFAULT_DATA_TOPIC, key, timedvalue);
				System.out.println("\t" + key +" > " + timedvalue.getTimestamp()+"\t|| "+timedvalue.getValue());
			}
			//store the tag, webID and timestamp of the final element
			addToHistory(key, webID, timedValues.get(timedValues.size()-1).getTimestamp());

			// [CMS]	
			System.out.println("\n\t[TAG] \t\t\t[Ultimo Timestamp KAFKA]");	
			System.out.println("\t" + key + "\t " + timedValues.get(timedValues.size()-1).getTimestamp());
			//System.out.println("\t TimestampFinalElement(produceToKafka): " + timedValues.get(timedValues.size()-1).getTimestamp());
		}
	}

	/**
	 * Gets a stream of values from the API, for a single PI Point.
	 * Retrieves (at most) the specified amount of values, between the start and end date.
	 * 
	 * @param webId - web ID of the element 
	 * @param start - Offset from current time, in minutes, of the last processed data point.
	 * @return A list of data points.
	 * @throws ApiException
	 */
	List<PITimedValue> getStream_raw(String webID, String start) throws ApiException {
		return client.getStream()
					 .getRecorded(webID, null, null,	null, /*Endtime is null as it defaults to '*' (present)*/ null, true, maxRecords, null, start, null)
					 .getItems();
	}
	
	/**
	 * Gets a stream set of values from the PI Points. This is a bulk call, returning a stream of values
	 * for every tag in the list. Will retrieve the n first data sets in the server,
	 * between the start and end times, from oldest to most recent.
	 * @param webIDs - A list of the web
	 * @return A streamset of PI Points
	 * @throws ApiException if the list of webIDs is too large, leading to error 414 (Request URI too long)
	 */
	List<PIStreamValues> getStreamSet_raw(List<String> webIDs) throws ApiException {
		return client.getStreamSet()
					 .getRecordedAdHoc(webIDs, null, endTime, null, true, maxRecords,null, null, null, startTime, null, null)
					 .getItems();
	}
	
	/** TODO Fix params for interval and sync time
	 * 
	 * Gets a stream of interpolated values from the API for a given tag, based on an interval.
	 * 
	 * @param webId - web ID of the element 
	 * @param start - Offset from current time, in minutes, of the last processed data point.
	 * @return A list of interpolated values
	 * @throws ApiException
	 */
	List<PITimedValue> getStream_interpolated(String webID, String start) throws ApiException {
		return client.getStream()
					 /*.getInterpolated(String webId, String desiredUnits, String endTime, String filterExpression, 
									  Boolean includeFilteredValues, String interval, String selectedFields, String startTime, 
									  String syncTime, String syncTimeBoundaryType, String timeZone)*/
					 .getInterpolated(webID, null, null, null, true,	"5m",null, start, "1h", null, null)
					 .getItems();
	}
	
	/**TODO Fix params for interval and sync time
	 * 
	 * Gets a stream set of interpolated values from the PI Points.
	 * This is a bulk call, returning a stream of values
	 * for every PI Point in the ID list.
	 * 
	 * @param webIDs - A list with the webIDs of the datastreams to retrieve
	 * @return A streamset of PI Points with interpolated values.
	 * @throws ApiException if the list of webIDs is too large, leading to error 414 (Request URI too long)
	 */
	List<PIStreamValues> getStreamSet_interpolated(List<String> webIDs) throws ApiException {
		return client.getStreamSet()
					 .getInterpolatedAdHoc(webIDs, endTime, null, true, "5m", null, null, null, startTime, "1h",	null, null, null)
					 .getItems();
	}
	
	/**
	 * Retrieves the n first values between the start date and the present time, for a given history entry.
	 * 
	 * @param key - kafka key of the tag
	 * @param entry - entry for the tag in the history map
	 * @return the number of values sent to Kafka
	 * @throws ApiException
	 */
	public List<PITimedValue> requestValues(String key, HistoryEntry entry) throws ApiException {
		String webID = entry.webID();
		String timestamp = entry.timestamp();
		
		List<PITimedValue> stream;
		switch(dataType) {
			default:					
			case RAW_DATA:
				stream = getStream_raw(webID, timestamp);
				break;
			case INTERPOLATED_DATA:
				stream = getStream_interpolated(webID, timestamp);
				break;
		}
		
		//check to remove the first element (last value of previous iteration)
		if(timestamp.equals(stream.get(0).getTimestamp())) 
			stream.remove(0);
		
		return stream;
	}
		
	/**
	 * Attempts to get all the values of a tag between the starting point and the current time.
	 * Due to API limitations over getting too many values at once, a specified number of values is retrieved at a time,
	 * stopping when the API returns less than that - as this means the last, latest value has been reached.
	 * @param key - Kafka key (format SERVER:TAG)
	 * @throws ApiException
	 */
	public void extract_crawl(String key) throws ApiException {
		// long t1 = System.nanoTime();
		// long sum = 0;
		// int i = 0;
		List<PITimedValue> stream;
		
		// if we ask the API for 10000 records and it only returns 500
		// that means there aren't any more than that and we can stop.
		do {
			HistoryEntry entry = history.get(key);
			stream = requestValues(key, entry);
			if(stream.isEmpty())
				break;	
			produceToKafka(key, entry.webID(), stream);
			// System.out.println(key+" (pass "+(++i)+") -- "+stream.size()+" values");
		} while(stream.size() >= maxRecords-1); //to make up for the discarded history record
		
		// long elapsed = (System.nanoTime()-t1) / 1000000000;
		// System.out.println("Reached the end of "+key+" in "+elapsed+" seconds ---- records produced: "+sum);
	}
	
	/**
	 * Searches a server for each query string and sends the results, if any, to Kafka.
	 * @param serverName
	 * @param searchQueries
	 * @throws IOException 
	 */
	public void extract_search(String serverName, String[] searchQueries) throws IOException {
		try {	//Some servers have restricted access, throwing an exception
			PIDataServer server = client.getDataServer().getByName(serverName, null, null);
			if(server.getIsConnected()) {//has no effect apparently, but left it in anyway.
				String serverWebID = server.getWebId();
				List<String> webIDs = getWebIDs( findTags(serverWebID, searchQueries) );
				if(webIDs.isEmpty()) {	//If no data points are found, skip the server.
					System.out.println("No matching records found in "+serverName);
					return;
				}
				else {
					List<PIStreamValues> streamset = null;
					switch(dataType) {
						default:					
						case RAW_DATA:
							streamset = getStreamSet_raw(webIDs);
							break;
						case INTERPOLATED_DATA:
							streamset = getStreamSet_interpolated(webIDs);
							break;
					}
					produceStreamSet(serverName, streamset);	
				}
			} else
				System.out.println(serverName+" is not connected.");
		}
		catch(ApiException e) {
			System.err.println(serverName+" --- "+e.getCode()+" : "+e.getMessage());
		}
	}

	/**
	 * Builds the path to a tag, retrieves its webID, and adds it to the history with a default timestamp.
	 * @param entry - a line of the .csv file: server, tag
	 */
	public void addTag(CSVEntry entry) {
		try {
			String path = new StringBuilder(PI_PATH_PREFIX_SERVER).append(entry.server()).append(PI_PATH_PREFIX_TAG).append(entry.tag()).toString();
			PIPoint point = client.getPoint().getByPath(path, null, null);
			String key = entry.server()+":"+entry.tag();
			history.put(key, new HistoryEntry(point.getWebId(), entry.timestamp()));
		} catch(Exception e) {
			// e.printStackTrace();
		}
	}

	/**
	 * Retrieves a list of all the data points in the server that match the search queries, up to {@code <maxPIPoints>} tags.
	 * The API does not support multiple query strings in one request, so each query is done individually.
	 * When using large amounts of points, requests may take a long time.
	 * Doing smaller, more frequent requests seems to perform better.
	 * 
	 * @param serverWebID - The web ID of the server being accessed
	 * @param queries - a list of search queries (tags)
	 * @return a list of PI Points
	 * @throws ApiException
	 */
	private List<PIPoint> findTags(String serverWebID, String[] queries) throws ApiException {
		List<PIPoint> points = new ArrayList<PIPoint>(maxPIPoints);//.getPoints(serverWebId, maxPiPoints, query, selectedFields, startIndex)
		if(queries == null){
		//	This would return the first <maxPIPoints> tags in the server. Uncomment if needed at some point.
		//	points.addAll( client.getDataServer().getPoints(serverWebID, maxPIPoints, null, null, null, null).getItems() );
		} else for(String query : queries) {
			points.addAll( client.getDataServer().getPoints(serverWebID, maxPIPoints, query, null, null, null).getItems() );
		}
		
		return points;
	}

	/**
	 * Returns the Web IDs of a given list of PI Points
	 * @param 	points 	a list of PI Points
	 * @return	a list of Web IDs (Strings)
	 */
	private List<String> getWebIDs(List<PIPoint> points) {
		List<String> webIDs = new ArrayList<String>(points.size()); 

		for(PIPoint point : points)
			webIDs.add(point.getWebId());

		return webIDs;
	}
	
	/**
	 * Retrieves all the servers listed in the API, and returns a list of their names.
	 * @return
	 * @throws ApiException
	 */
	public List<String> getServerList() throws ApiException {
		List<PIDataServer> servers = client.getDataServer().list(null, null).getItems();
		List<String> serverNames = new ArrayList<>(servers.size());
		
		for(PIDataServer server : servers)
			serverNames.add(server.getName());
		
		return serverNames;
	}

	/**
	 * Parses an incoming message from a channel into a PITimedValue object containing the value.
	 * The rest of the message can be discarded, as tag/webid info is tied to the listener anyway.
	 * 
	 * @param message - incoming JSON message
	 * @return PITimedValue contained in the message
	 */
	public PITimedValue parseMessage(String message) {
		PITimedValue value = null;	
		try {
			JsonNode root = mapper.readTree(message);
			JsonNode val = root.get("Items").get(0).get("Items").get(0);
			value = mapper.treeToValue(val, PITimedValue.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return value;
	}
	
	// The API client's implementation of this seems to be broken, as it always sets the protocol to https.
	// Implemented it manually with a WebSocket, values are parsed from the JSON response.
	
	/**
	 * Creates a new WebSocket to connect to a Channel of the PI Web API, and listens for updates to those tags.
	 * After a set threshold, the list of new values is sent to Kafka.
	 * Channels send a message whenever the datastream is updated with a new value.
	 * 
	 * NOTE: A value CAN be lost here, if a new value is added to the system
	 * between the end of the previous request and the WebSocket starting, however unlikely that may be.
	 * This could be avoided by setting includeInitialValues to true, which sends an initial message with the current value of the tag,
	 * however doing so will introduce duplicate values for other tags.
	 * 
	 * @param webID
	 * @throws WebSocketException
	 * @throws IOException
	 */
	public void startChannel(String webID, String key) throws WebSocketException, IOException {
		WebSocket socket = new WebSocketFactory().createSocket("wss://"+API_URL+"/streams/"+webID+"/channel?includeInitialValues=false")
				.addListener(new WebSocketAdapter(){
					@Override
					public void onTextMessage(WebSocket ws, String message) {
						PITimedValue value = parseMessage(message);
						producer.send(DEFAULT_DATA_TOPIC, key, value);
						addToHistory(key, webID, value.getTimestamp());
						//System.out.println(key + " has been updated. ["+value.getTimestamp()+"]");
					}
					@Override
					public void onConnected(WebSocket websocket, Map<String, List<String>> headers) {
						//System.out.println("Channel started for "+tag);
					}
					@Override
					public void onDisconnected(WebSocket websocket, WebSocketFrame serverCloseFrame, WebSocketFrame clientCloseFrame, boolean closedByServer) {
						//System.out.print("Disconnected from channel for "+tag);
					}
				})
				.setUserInfo(username, password);
			
			socket.connect();
	}
}