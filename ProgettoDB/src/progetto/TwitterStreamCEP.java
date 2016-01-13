package progetto;

import java.io.File;
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

	public static void main(String[] args) {
		
		try {
			
			createEsperRuntime("../ProgettoDB/src/Progetto/query2.epl");
			listenToTwitterStream(createTwitterStream());
		} catch (TwitterException | IOException e) {
			e.printStackTrace();
		}
	}

	public static class CEPListener implements UpdateListener {
		
		public void update(EventBean[] newData, EventBean[] oldData) {
			try {
//				if (newData == null) {
//					return;
//				}
				EventBean event = newData[0];
//				System.out.println("EVENT: " + event.get("user") + " " + event.get("timeZone") +  " " + event.get("lang"));
					//for(EventBean event: newData){
						System.out.println("EVENT: " + event.getUnderlying());
					//}
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
	 * Creates and return object from Twitter stream factory.
	 * 
	 * @return Twitter stream object.
	 */
	public static TwitterStream createTwitterStream() {
		
		ConfigurationBuilder builder = new ConfigurationBuilder();
		builder.setOAuthConsumerKey(CONSUMER_KEY);
		builder.setOAuthConsumerSecret(CONSUMER_SECRET);
		//Configuration configuration = (Configuration) builder.build();
		TwitterStreamFactory factory = new TwitterStreamFactory();
		TwitterStream twitterStream = factory.getInstance();
		
		//TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		// Set access by OAuth
		//twitterStream.setOAuthConsumer(CONSUMER_KEY, CONSUMER_SECRET);
		//twitterStream.setOAuthAccessToken(new AccessToken(ACCESS_TOKEN, ACCESS_TOKEN_SECRET));


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
				UserMentionEntity[] mentions  = status.getUserMentionEntities();
				for(UserMentionEntity mention : mentions){
					TwitterBean twitterBean = new TwitterBean(status, mention);				
					cepRuntime.sendEvent(twitterBean);
				}
//				System.out.println(status.getUser().getName() + ": " + status.getText());
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

		twitterStream.addListener(listener);
		twitterStream.sample();
	}

}
