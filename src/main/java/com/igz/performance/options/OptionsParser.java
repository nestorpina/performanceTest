package com.igz.performance.options;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.igz.performance.database.DatabaseFactory.DatabaseType;
import com.igz.performance.queue.QueueFactory.QueueType;

public class OptionsParser {
	
	private static final String CSV = "csv";
	private static final String HELP = "help";
	private static final String WORKERS = "workers";
	private static final String DEBUG = "debug";
	private static final String NUM_EVENTS = "n";
	private static final String QUEUE = "queue";
	private static final String DATABASE = "database";
	
	private static final String DEFAULT_NUM_EVENTS = "50000";
	private static final String DEFAULT_WORKERS = "10";
	private static final String DEFAULT_QUEUE = "NONE";
	private static final String DEFAULT_DATABASE = "MYSQL";
	
	CommandLine parsedOptions = null;

	public com.igz.performance.options.Options parseCommandLineOptions(String[] args) throws ParseException {
		Options options = createOptions();
		com.igz.performance.options.Options igzOptions;
		CommandLineParser parser = new GnuParser();
		
		try {
			parsedOptions = parser.parse( options, args);
			igzOptions = buildOptions(parsedOptions);
		} catch (ParseException e1) {
				System.out.println(e1.getMessage());
				System.out.println();
				printHelp();
				throw e1;
		}
		
		return igzOptions;
	}

	@SuppressWarnings("static-access")
	private Options createOptions() {
		Option count  = OptionBuilder.withArgName( "N" )
	            .hasArg()
	            .withDescription(  "N: number of events to execute (insert/select operations). Default : 50000" )
	            .withType(Integer.class)
	            .withLongOpt("num-events")
	            .create( NUM_EVENTS );
		Option workers  = OptionBuilder.withArgName( "N" )
	            .hasArg()
	            .withDescription(  "N: number of workers (threads/consumers) that execute operations. Default : 10 " )
	            .withType(Integer.class)
	            .create( WORKERS );
		Option queue = OptionBuilder.withArgName( "QUEUE" )
	            .hasArg()
	            .withDescription(  "set the QUEUE system to use, valid values : [RABBITMQ, ZEROMQ (default), AMAZONSQS, NONE] or leave empty to don't use a queue system" )
	            .create( QUEUE );
		Option database = OptionBuilder.withArgName( "DATABASE" )
	            .hasArg()
	            .withDescription(  "set the DATABASE system to use, valid values : [MYSQL, MONGODB, REDIS (default), SQLSERVER]" )
	            .withType(DatabaseType.class)
	            .create( DATABASE );
		Option usage = OptionBuilder
	            .withDescription(  "show this help" )
	            .withLongOpt(HELP)
	            .create( HELP );
		Option csv = OptionBuilder
	            .withDescription(  "Output values for csv import/export" )
	            .withLongOpt("optput-csv")
	            .create( CSV );
		Option debug = new Option(DEBUG,"activate debug mode");
		Options options = new Options();
		options.addOption(count);
		options.addOption(workers);
		options.addOption(queue);
		options.addOption(database);
		options.addOption(debug);
		options.addOption(usage);
		options.addOption(csv);
		return options;
	}
	
	

	public void printHelp() {
		HelpFormatter formatter = new HelpFormatter();
		Options options = createOptions();
		formatter.printHelp( "java -Djava.library.path=/usr/local/lib -jar ptest-jar-with-dependencies.jar [OPTIONS]", options );
	}
	
	private com.igz.performance.options.Options buildOptions(CommandLine parsedOptions) throws ParseException {
		com.igz.performance.options.Options options = new com.igz.performance.options.Options();
		
		try {
			DatabaseType databaseType = DatabaseType.valueOf(parsedOptions.getOptionValue(DATABASE, DEFAULT_DATABASE));
			options.setDatabaseType(databaseType);
		} catch (IllegalArgumentException e) {
			throw new ParseException("Invalid database option : " + parsedOptions.getOptionValue(DATABASE));
		}

		try {
			QueueType queueType = QueueType.valueOf(parsedOptions.getOptionValue(QUEUE, DEFAULT_QUEUE));
			options.setQueueType(queueType);
		} catch (IllegalArgumentException e) {
			throw new ParseException("Invalid queue option : " + parsedOptions.getOptionValue(QUEUE));
		}

		try {
			options.setNumEvents(Integer.parseInt(parsedOptions.getOptionValue(NUM_EVENTS,DEFAULT_NUM_EVENTS)));
		} catch( NumberFormatException e) {
			throw new ParseException("Argument is not a number for option: num");
		}
		try {
			options.setNumWorkers(Integer.parseInt(parsedOptions.getOptionValue(WORKERS,DEFAULT_WORKERS)));
		} catch( NumberFormatException e) {
			throw new ParseException("Argument is not a number for option: workers");
		}
		options.setShowHelp(parsedOptions.hasOption(HELP));
		options.setDebug(parsedOptions.hasOption(DEBUG));
		options.setOutputCsv(parsedOptions.hasOption(CSV));

		return options;
	}

}
