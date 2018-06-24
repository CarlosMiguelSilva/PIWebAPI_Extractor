import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import com.osisoft.pidevclub.piwebapi.ApiException;

import extractor.CSVEntry;
import extractor.PIWebApiExtractor;
import hbase.HistoryEntry;
import hbase.HistoryManager;
import producer.KafkaPIDataProducer;

/**
 * Data extractor for the OSI PI system using the PI Web API.
 */
public class Main {
	
	/* DEBUG FLAG */
	//enables the API's debugging mode which shows request info
	public static boolean DEBUG = false; 
	
	/* MODES */
	public static final int MODE_MANUAL_SEARCH = 1;
	public static final int MODE_UPDATE = 2;
	public static final int MODE_CRAWL = 3;
	
	/* DEFAULT: */
	//Timestamp for new map entries.
	static final String DEFAULT_TIMESTAMP = "2000-01-01T00:00:00.000000Z";
	//Start and End time for searching
	static final String DEFAULT_START_TIME = "2018-01-01T00:00:00.000000Z";
	static final String DEFAULT_END_TIME = "*";
	//Maximum number of records to retrieve per request
	static final int DEFAULT_MAX_RECORDS = 10000;
	//Number of history entries
	static final int DEFAULT_MAP_SIZE = 1000; 
	//Maximum number of tags returned by the search [only applied to manual search] 
	static final int DEFAULT_MAX_TAGS = 1;
	//Time in ms between updating the history table in HBase
	static final int SLEEP_TIMER = 15000;
	/* VARIABLES */
	private static String startTime = DEFAULT_START_TIME;
	private static String endTime = DEFAULT_END_TIME;
	private static String[] queries = null;
	private static int maxRecords = DEFAULT_MAX_RECORDS;
	private static int maxPoints = DEFAULT_MAX_TAGS;
	private static List<String> servers;
	private static int mode;
	private static int dataType;
	private static Options options;
	private static String filename;
	private static boolean input_csv = false;
	private static PIWebApiExtractor extractor;
	private static Map<String, HistoryEntry> history = new ConcurrentHashMap<>(DEFAULT_MAP_SIZE);
	private static List<String> filterKeySet = new ArrayList<>();
	public static String CSV_SEPARATOR = ";";
	
	public static void main(String[] argv) throws InterruptedException, IOException  {
		
		//BasicConfigurator.configure();
		
		/* USER INPUT: Username & Password */
		//String password = new String(System.console().readPassword());
		//System.out.println("Password for user "+username+":"); 		
		String username = argv[0];
		String password = argv[1];
	
		//Parse arguments
		options = setupOptions();
		CommandLine cmd;
		try {
			cmd = new DefaultParser().parse(options, argv);
			parseOptions(cmd);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		///////////////////////////////////////////////////   KAFKA Producer    /////////////////////////////////////////////////////////
		System.out.println("Starting Kafka producer.");
		KafkaPIDataProducer producer = new KafkaPIDataProducer();
		
		/////////////////////////////////////////////////////// PIWeb API /////////////////////////////////////////////////////////////// 
		System.out.print("Authenticating to PI Web API... ");
		try {
			extractor = new PIWebApiExtractor(username, password, producer,	startTime, endTime, maxRecords, maxPoints, dataType, DEBUG);
			System.out.println("[OK]");
		} catch(ApiException e) {
			System.err.println("Failed. Quitting...");
			producer.close();
			System.exit(1);
		}

		///////////////////////////////////////////////   HBase History Function   //////////////////////////////////////////////////////
		System.out.print("Connecting to HBase...\n");
		HistoryManager hb = null;
		try {
			hb = new HistoryManager();	
			System.out.println("Loading data access history...");	
			history = hb.getHistoryMap();
			
			extractor.setHistoryMap(history);
			System.out.println("Done.");				
		} catch (IOException e) {
			e.printStackTrace();
			producer.close();
			
			System.exit(1);
		}
		
		// IF(csv file was passed as an argument):
		// Look for new tags and add new history entries
		if(input_csv) {
			System.out.print("Checking new entries... ");
			List<CSVEntry> csv = parseCSV();
			if(csv.size() > 0) {
				for(CSVEntry entry : csv)
					extractor.addTag(entry);
				history = extractor.getHistoryMap();
				hb.writeHistory(history);
				System.out.println("[Done]");
				
				
				//Filter history so that the only entries extracted are those in the CSV
				history.keySet().retainAll(filterKeySet);
				extractor.setHistoryMap(history);
			} else {
				System.out.print("[None found]");
			}
		}
		

		
		
		/* Shutdown hook */
		// Requires a copy of the manager as a final variable
		final HistoryManager hbase = hb;
		//Creates a shutdown hook to try and save the offset map on program shutdown, whether it finishes running 
		//or someone ctrl+c's the terminal. Does NOT work in eclipse, as the stop button just kills the JVM instantly.
		Runtime.getRuntime().addShutdownHook(
			new Thread(()->{
				try {
					System.out.print("Quitting. Saving history... ");
					hbase.writeHistory(extractor.getHistoryMap());
					System.out.println("Done.");
				} catch(Exception e) {
					e.printStackTrace();
				}
			})
		);
		
		//Periodically update the history in HBase 
		//Sleeps first to avoid having a pointless update at the start
		Thread historyUpdater = new Thread(() -> {
			try {
				Thread.sleep(SLEEP_TIMER);
				hbase.writeHistory(extractor.getHistoryMap());
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		});
		
		
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		ExecutorService threadPool = null;
		historyUpdater.start();
		switch(mode) {
			//searches for any tags in a server - or all of them - matching the query string
			case MODE_MANUAL_SEARCH:
				if(servers == null) {
					System.out.print("No servers listed - retrieving server list from API... ");
					try {
						servers = extractor.getServerList();
					} catch (ApiException e) {
//						e.printStackTrace();
					}
					System.out.println("Done.");
				}
				//One thread per server
				threadPool = Executors.newFixedThreadPool(servers.size());
	
				for(String server : servers) {
					threadPool.submit( () -> {
						try {
							extractor.extract_search(server, queries);
						} catch (Exception e) {
							e.printStackTrace();
						}
					});
				}
				break;
			default:
			case MODE_CRAWL:
				//TODO balance number of threads
				threadPool = Executors.newCachedThreadPool();
					//For each tag, crawl through all recent data, then open a channel
					for(String key : history.keySet()) {
						threadPool.submit(() -> {
							try {
								extractor.extract_crawl(key);
								extractor.startChannel(history.get(key).webID(), key);
							} catch(Exception e) {
							//	e.printStackTrace();
							}
						});
				}
				break;
		}
		
		/////////Cleanup///////
		threadPool.shutdown();
		try { 
			// Wait indefinitely for tasks to finish
			threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch(InterruptedException e) {
			e.printStackTrace();
		}		
		
		
		//Wait indefinitely, to keep the websockets open.
		while(true) {
			Thread.sleep(5000);
		}
		
		//TODO add a way to finish execution?
//		producer.close();
//		System.out.println("Quitting.");
	}
	
	
	/////////////////////////// Arguments - using Apache Commons CLI //////////////////////////////
	
	/**
	 * Prints a message explaining how to use the application.
	 */
	private static void instructions() {
		new HelpFormatter().printHelp("Extract <username> [-csv <filename>] [-r <max records>]"
				+ "[-search [-q <queries>  -s <servers> -n <max tags> -t <start date> [end date] ] ]", options);
		System.out.println("<queries> is a list of one or more query strings separated by commas.\n" +
				"  \n '*' and '?' may be used as wildcards. Read /piwebapi/help/topics/query-string" +
				"  \n<servers> is a list of one or more PI server names to retrieve data from.\n" +
				"Example: Extractor user1 -q ALQ?.M?*,*M2*,PHFZ* -s EDPPORSKIP1 EDPFRASKIP1");
	}

	/**
	 * Sets up the options for the command line parser.
	 * @return
	 */
	private static Options setupOptions() {
		options = new Options();
		
		options.addOption("help", false, "Displays this help message.");
		options.addOption("debug", false, "Enables debugging for the PI Web API client.");
	
		//function modes
		options.addOption("search", false, "Searches across servers for tags matching the input queries");
		options.addOption("csv", true, "Path to a .csv file containing a list of tags. Format: SERVER;TAG[;STARTDATE]");
		options.addOption("r", true, "Max number of records to retrieve from each point");
		options.addOption("n", true, "Max number of PI Points");
		options.addOption("interpolated", false, "Sets the extractor to retrieve interpolated (instead of raw) data points");
//		options.addOption("raw", false, "Sets the extractor to retrieve raw data points. Enabled by default.");
		
		Option time = Option.builder("t").desc("Start time. Optionally, an end date can also be specified.").numberOfArgs(2).optionalArg(true).build();
		options.addOption(time);

		Option serverList = new Option("s", true, "List of API servers to be searched");
		serverList.setArgs(Option.UNLIMITED_VALUES);
		options.addOption(serverList);

		Option queries = new Option("q", true, "Query strings (tags)");
		queries.setArgs(Option.UNLIMITED_VALUES);
		options.addOption(queries);
		
		return options;
	}
	
	/**
	 * Reads in the arguments from the command line.
	 * @param cmd
	 */
	private static void parseOptions(CommandLine cmd) {
		if(cmd.hasOption("help")) {
			instructions();
			System.exit(0);
		}

		if(cmd.hasOption("debug"))
			DEBUG = true;
		
		if(cmd.hasOption("csv")) {
			input_csv = true;
			filename = cmd.getOptionValue("csv");
		}
		
		if(cmd.hasOption("search"))
			mode = MODE_MANUAL_SEARCH;
		
		if(cmd.hasOption("interpolated"))
			dataType = PIWebApiExtractor.INTERPOLATED_DATA;
		
		// Max number of records
		if(cmd.hasOption('r'))
			maxRecords = Integer.parseInt(cmd.getOptionValue('r'));
				
		// Query strings
		if(cmd.hasOption('q'))
			queries = cmd.getOptionValues('q');

		// Server names
		if(cmd.hasOption('s'))
			servers = Arrays.asList(cmd.getOptionValues('s'));

		// Start & End time
		if(cmd.hasOption('t')) {
			String[] time = cmd.getOptionValues('t');
			startTime = time[0];
			if(time.length == 2)
				endTime = time[1];
		}
		
		// Max number of tags
		if(cmd.hasOption('n'))
			maxPoints = Integer.parseInt(cmd.getOptionValue('n'));
	}
	
	/**
	 * Parses the .csv config file into a list of <server,tag> pairs.
	 * Adds new entries, if any, to the history. All others are ignored.
	 * Optionally, you can add a starting timestamp for a tag.
	 * This timestamp can be in ISO8001 format, or written as an offset (such as "*-10y").
	 * The csv has a header line which gets discarded. Remove if needed.
	 * 
	 * Format: <pre>{@code SERVER;TAG[;TIMESTAMP]}</pre>
	 * 
	 * @return List of csv entries with server, tag, and initial timestamp info.
	 * @throws IOException if an error occurs while reading the .csv
	 */
	private static List<CSVEntry> parseCSV() throws IOException {
		List<CSVEntry> csv_entries = new ArrayList<>();
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line = br.readLine(); //discard header line
		String[] config;
		String timestamp;
		
		while((line = br.readLine()) != null) {
			config = line.split(CSV_SEPARATOR);
			//Skip X's in the file
			if(config[0].equalsIgnoreCase("X"))
				continue;

			String key = config[0]+":"+config[1];
			filterKeySet.add(key);
			if(history.containsKey(key))
				continue;
			
			//if the line has a timestamp use it, else use default one
			if(config.length==3)
				timestamp = config[2];
			else
				timestamp = DEFAULT_TIMESTAMP;
			
			//Server, tag, timestamp
			csv_entries.add(new CSVEntry(config[0], config[1], timestamp));
		}
		br.close();
		
	return csv_entries;
	}

}
