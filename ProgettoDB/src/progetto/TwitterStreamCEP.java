package progetto;

import java.io.File;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.UserMentionEntity;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterStreamCEP {
	
	static EPRuntime cepRuntime;
	static private String CONSUMER_KEY="8vx4ITb7poj4KM1vX7PiXI3Ms";
	static private String CONSUMER_SECRET="IqH3wrYTMjz5CGyDwATyjEDbTFieoonII4UWIzA1Qg2nsoPUeb";
	static private String ACCESS_TOKEN="4746987855-79dPGcxhOcalYRVkpDD36gwEa47ShNqarck53tO";
	static private String ACCESS_TOKEN_SECRET="AEHbX4ddL1vi6j4uTM8y4tkU1Y5c0rRb0OqWub4vhlYuH";

	private static String userInput(){
		String selection = "Select a query to perform by typing the corresponding number :"+
				"\n1 - query.epl -> Most mentioned user, window 1 minute, snapshot 30 seconds"+
				"\n2 - query2.epl -> Most mentioned user, window 5 seconds, snapshot 3 seconds";
		System.out.println(selection);
	    Scanner input = new Scanner(System.in);
	    String query = input.nextLine();
	    boolean condition = false;
	    if(Integer.parseInt(query) < 1 || Integer.parseInt(query) > 2){
	    	condition = true;
	    }
	    while(condition){
	    	condition = false;
	    	System.out.println("WRONG INPUT");
	    	System.out.println(selection);
	    	query = input.next();
	    	if(Integer.parseInt(query) < 1 || Integer.parseInt(query) > 2){
		    	condition = true;
		    }
	    }
	    String returnqQuery = "query.epl";
	    switch(Integer.parseInt(query)){
	    case 1 : returnqQuery = "query.epl"; break;
	    case 2: returnqQuery = "query2.epl"; break;
	    }
		return returnqQuery;
	}
	
	public static void main(String[] args) {
		
		SimpleLayout layout = new SimpleLayout();
	    ConsoleAppender appender = new ConsoleAppender(new SimpleLayout());
	    Logger.getRootLogger().addAppender(appender);
	    Logger.getRootLogger().setLevel((Level) Level.WARN);
	    
		try {
			/*The following method takes in input the source file containing the query; 
			 *
			 * the attached files are :
			 *
			 * query.epl -> show a snapshot every thirty seconds of the most mentioned user 
			 * within all the tweets received in the last minute
			 * 
			 * query2.epl -> show a snapshot every 1.5 seconds of the most mentioned user 
			 * within all the tweets received in the last 3 seconds
			 * 
			 */
			createEsperRuntime("../ProgettoDB/src/Progetto/" + userInput());
			listenToTwitterStream(createTwitterStream());
		} catch (TwitterException | IOException e) {
			e.printStackTrace();
		}
	}

	public static class CEPListener implements UpdateListener {
		
		public void update(EventBean[] newData, EventBean[] oldData) {
			try {
				if(newData != null){
					System.out.println("Query performed");
					for(EventBean event : newData){
						System.out.println("The user most mentioned was : " + " " + event.getUnderlying().toString());
						}
					}
				} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}	
	
	public static EPRuntime createEsperRuntime(String queryFileName) throws FileNotFoundException {
		// Configuration
		Configuration cepConfig = new Configuration();
		cepConfig.addEventType("TwitterBean", TwitterBean.class.getName());
		// Setup provider, runtime and administrator
		EPServiceProvider cep = EPServiceProviderManager.getProvider("TwitterStreamCEP", cepConfig);
		cepRuntime = cep.getEPRuntime();
		EPAdministrator cepAdmin = cep.getEPAdministrator();
		
		System.out.println("------ Performing Query ------");

		String query = new Scanner(new File(queryFileName)).useDelimiter("\\Z").next();
		cepAdmin.destroyAllStatements();
		EPStatement cepStatement = cepAdmin.createEPL(query);
		cepStatement.addListener(new CEPListener());
		
		return cepRuntime;
	}	
	
	/**
	 * Creates and return object from Twitter stream factory
	 * @return Twitter stream object.
	 */
	public static TwitterStream createTwitterStream() {
		ConfigurationBuilder builder = new ConfigurationBuilder();
		builder.setOAuthConsumerKey(CONSUMER_KEY);
		builder.setOAuthConsumerSecret(CONSUMER_SECRET);
		builder.setOAuthAccessToken(ACCESS_TOKEN);
		builder.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET);
		TwitterStreamFactory factory = new TwitterStreamFactory(builder.build());
		TwitterStream twitterStream = factory.getInstance();
		return twitterStream;
	}
	
	/**
	 * Print Twitter stream.
	 * 
	 * @param twitterStream
	 *            Object from TwitterStreamFactory
	 * @throws TwitterException
	 * @throws IOException
	 */
	public static void listenToTwitterStream(twitter4j.TwitterStream twitterStream)
			throws TwitterException, IOException {
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				//for every mention in the tweet we generate an object
				UserMentionEntity[] mentions  = status.getUserMentionEntities();
				System.out.println("tweet received, mentions contained:---------");
				for(UserMentionEntity mention : mentions){
					System.out.println(mention.getName());
				}
				System.out.println("--------------------------------------------");
				for(UserMentionEntity mention : mentions){
					TwitterBean twitterBean = new TwitterBean(status, mention);				
					cepRuntime.sendEvent(twitterBean);
				}
			}

			@Override
			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
					//System.out.println("Got status deletion notice - id:" + statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				//System.out.println("Got track limitation notice: " + numberOfLimitedStatuses);
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				//System.out.println("Got scrub geo - userId:" + userId + " upToStatusId:" + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning stallWarning) {
				// System.out.println("Got stall warning :" + stallWarning);
			}
		};

		long 	ansa = 150725695, 
				masterchef = 222908821,
				music_as_life = 1693516848,
				disney = 67418441,
				hearthstone = 1209608880,
				leo_dicaprio = 133880286,
				youtube	= 10228272,
				gli_stockisti = 480312711,
				fedez = 267138741, //il cantante
				fedex = 134887156,//i pacchi
				starbucks = 30973,
				la_zanzara = 409500620; 
		//da ragazzine
		long 	zayn = 176566242,
				bieber = 27260086;
		
		long[] query = {ansa, masterchef, music_as_life, la_zanzara,
						disney, hearthstone, leo_dicaprio, youtube,
						gli_stockisti, starbucks, fedex, fedez, zayn, bieber};
		
		twitterStream.addListener(listener);
		twitterStream.filter(new FilterQuery(query));
	}

}
