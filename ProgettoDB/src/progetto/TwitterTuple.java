package progetto;

import java.util.ArrayList;
import java.util.Date;

import twitter4j.Status;
import twitter4j.UserMentionEntity;

public class TwitterTuple {

	private String user;
	private String lang;
	private double latitude;
	private double longitude;
	private String mentionedUser;
	private UserMentionEntity mentions;

	/**
	 * CONSTRUCTOR
	 * @param status : the actual tweet present in the portion of the stream received
	 * @param mention : one of the multiple mentions present in the status
	 */
	public TwitterTuple(Status status, UserMentionEntity mention) {
		this.user = status.getUser().getScreenName();
		this.lang = status.getUser().getLang();
		this.mentions = mention;
		this.mentionedUser = mention.getScreenName();
		if( status.getGeoLocation() == null){
			this.latitude = -999.0;
			this.longitude = -999.0;
		}
		else { 
			this.latitude = status.getGeoLocation().getLatitude();
			this.longitude = status.getGeoLocation().getLongitude();	
		}
	}
	
/**
 * GETTERS AND SETTERS	
 * 
 */
	public UserMentionEntity getMentions() { return mentions;}
	public void setMentions(UserMentionEntity mentions) {this.mentions = mentions;}
	public String getMentionedUser() {return mentionedUser;}
	public void setMentionedUser(String mentionedUser) {this.mentionedUser = mentionedUser;}
	public double getLatitude() {return latitude;}
	public void setLatitude(double latitude) {this.latitude = latitude;}
	public double getLongitude() {return longitude;}
	public void setLongitude(double longitude) {this.longitude = longitude;}
	public String getLang() {return lang;}
	public void setLang(String lang) {this.lang = lang;}
	public String getUser() {return user;}
	public void setUser(String user) {this.user = user;}

}
