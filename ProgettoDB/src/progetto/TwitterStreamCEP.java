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
	//variable for letting the user test the four queries 
	static private int cQuery;
	
	public static void main(String[] args) {
		//SimpleLayout layout = new SimpleLayout();
	    ConsoleAppender appender = new ConsoleAppender(new SimpleLayout());
	    Logger.getRootLogger().addAppender(appender);
	    Logger.getRootLogger().setLevel((Level) Level.WARN);
		try {
			/*The following method takes in input the source file containing the query; */
			createEsperRuntime("../ProgettoDB/src/Progetto/" + userInput());
			listenToTwitterStream(createTwitterStream());
		} catch (TwitterException | IOException e) {e.printStackTrace();}
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
		cepConfig.addEventType("TwitterTuple", TwitterTuple.class.getName());
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
		builder.setHttpConnectionTimeout(10000);
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
				/*
				if(mentions.length > 0)System.out.println("tweet received, mentions contained:---------");
				for(UserMentionEntity mention : mentions){
					
					if(cQuery == 3 && status.getGeoLocation() != null){
						System.out.println("\t\t" + mention.getScreenName()+"\t tweeted at : "+
										status.getGeoLocation());}
				}
				if(mentions.length > 0)System.out.println("--------------------------------------------");
				*/
				//filtro sui mentioned user, tenendo solo i tweet con mentions
				for(UserMentionEntity mention : mentions){
					//filtro sulla location
					if(cQuery == 3 && status.getGeoLocation() != null){
						TwitterTuple twitterTuple = new TwitterTuple(status, mention);
						cepRuntime.sendEvent(twitterTuple);
					}
					else if(cQuery != 3){
						TwitterTuple twitterTuple = new TwitterTuple(status, mention);
						cepRuntime.sendEvent(twitterTuple);
					}		
				}
			}
			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {/*DOSOMETHING*/}
			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {/*DOSOMETHING*/}
			@Override
			public void onException(Exception ex) {ex.printStackTrace();}
			@Override
			public void onScrubGeo(long userId, long upToStatusId) {/*DOSOMETHING*/}
			@Override
			public void onStallWarning(StallWarning stallWarning) {/*DOSOMETHING*/}
		};
		twitterStream.addListener(listener);
		switch(cQuery){
			case 1: twitterStream.sample(); break;
			case 3: { 
				//creo un filtro per lo strem
				FilterQuery filterQuery = new FilterQuery();
				/*
				 filtrare in base alla geolocation, usiamo un bounding box
				 boundingbox -> prima angolo sin poi angolo dx
				 utility per calcolare i dati da inserire:
				 http://tools.geofabrik.de/calc/#type=geofabrik_standard&bbox=9.065263,45.393007,9.302486,45.5421&tab=1&proj=EPSG:4326&places=2
				 es : MILANO : {9.06,45.39},{9.31,45.55}
				 */
				//whole USA coordinates (long,lat)
				double[][] bb= {{-128.94, 21.96}, {-58.44, 48.73}};	
				filterQuery.locations(bb); twitterStream.filter(filterQuery); break;
			}
			case 2: {
				//creo un filtro per lo strem
				FilterQuery filterQuery = new FilterQuery();
				/*
				 filtrare in base all'id degli user
				 
				 utility per calcolare i dati da inserire:
				 http://mytwitterid.com/
				*/
				long 	ansa = 150725695, leo_dicaprio = 133880286, gli_stockisti = 480312711,
						youtube	= 10228272, la_zanzara = 409500620, masterchef = 222908821,
						music_as_life = 1693516848,	hearthstone = 1209608880, starbucks = 30973,
						disney = 67418441, zayn = 176566242, fedez = 267138741, //il cantante 
						fedex = 134887156,//i pacchi
						bieber = 27260086; 
	
				long[] followQuery = {ansa, masterchef, music_as_life, la_zanzara,
						disney, hearthstone, leo_dicaprio, youtube,
						gli_stockisti, starbucks, fedex, fedez, zayn, bieber};
				
				filterQuery.follow(followQuery); twitterStream.filter(filterQuery); break;
			}
			case 4: {
				//creo un filtro per lo strem
				FilterQuery filterQuery = new FilterQuery();
				//singers
				long 	zayn = 176566242,
						bieber = 27260086,
						selena_gomez = 23375688,
						miley = 268414482,
						shakira = 44409004,
						eminem = 22940219,
						rihanna = 79293791,
						madonna = 512700138;
				long[] singerQuery={miley,zayn,bieber,selena_gomez,madonna,
						rihanna,shakira,eminem};
				filterQuery.follow(singerQuery); twitterStream.filter(filterQuery); break;
			}
			default : break;
	}
	}

	
	private static String userInput(){
		String selection = "Select a query to perform by typing the corresponding number :"+
				"\n1 - Sample.epl -> Select 4 Most mentioned users over the sample stream, window 1 minute, snapshot 30 seconds"+
				"\n2 - Follow.epl -> 4 Most mentioned users over a set of pages"+
				"\n3 - Location.epl -> select users and their location; tweets come from whole USA, query performed on the west coast zone"+
				"\n4 - Follow_Singers.epl -> 2 Most mentioned singers from a set of chosen ones";
		System.out.println(selection);
	    Scanner input = new Scanner(System.in);
	    String query = input.nextLine();
	    boolean condition = false;
	    if(Integer.parseInt(query) < 1 || Integer.parseInt(query) > 4){
	    	condition = true;
	    }
	    while(condition){
	    	condition = false;
	    	System.out.println("WRONG INPUT");
	    	System.out.println(selection);
	    	query = input.next();
	    	if(Integer.parseInt(query) < 1 || Integer.parseInt(query) > 4){
		    	condition = true;
		    }
	    }
	    String returnqQuery = "query.epl";
	    cQuery=Integer.parseInt(query);
	    switch(cQuery){
	    case 1: returnqQuery = "Sample.epl"; break;
	    case 2: returnqQuery = "Follow.epl"; break;
	    case 3: returnqQuery = "Location.epl"; break;
	    case 4: returnqQuery = "Follow_Singers.epl"; break; 
	    }
		return returnqQuery;
	}
	
	
}
