package progetto;

import java.util.ArrayList;

import twitter4j.Status;
import twitter4j.UserMentionEntity;

public class TwitterBean {

	private String user;
	private String lang;
	private int followersCount;
	private String timeZone;
	private double latitude;
	private double longitude;
	private int UTcOffset;
	private String text;
	private String mentionedUser;
	public String getMentionedUser() {
		return mentionedUser;
	}

	public void setMentionedUser(String mentionedUser) {
		this.mentionedUser = mentionedUser;
	}

	private UserMentionEntity mentions;

	
	
	public int getFollowersCount() {
		return followersCount;
	}

	public void setFollowersCount(int followersCount) {
		this.followersCount = followersCount;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public int getUTcOffset() {
		return UTcOffset;
	}

	public void setUTcOffset(int uTcOffset) {
		UTcOffset = uTcOffset;
	}

	public TwitterBean() {
		
	}
	
	public TwitterBean(Status status, UserMentionEntity mention) {
		this.user = status.getUser().getScreenName();
		this.lang = status.getUser().getLang();
		this.followersCount = status.getUser().getFollowersCount();
		this.mentions = mention;
		this.mentionedUser = mention.getScreenName();
		if( status.getGeoLocation() == null){
			this.latitude = -999.0;
			this.longitude = -999.0;
		}
		else { 
			this.latitude = status.getGeoLocation().getLongitude();
			this.longitude = status.getGeoLocation().getLatitude();	
		}
		this.UTcOffset = status.getUser().getUtcOffset();
		this.timeZone = status.getUser().getTimeZone();
		this.text = status.getText();		
	}

	public String getLang() {
		return lang;
	}

	public void setLang(String lang) {
		this.lang = lang;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}
	
	public String getTimeZone() {
		return timeZone;
	}

	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}

	@Override
	public String toString() {
	/*	return "TwitterBean [user=" + user + ", lang=" + lang
				+ ", followersCount=" + followersCount + ", timeZone="
				+ timeZone + ", latitude=" + latitude + ", longitude="
				+ longitude + ", UTcOffset=" + UTcOffset + "]";
	*/
	
		return "TwitterBean [user = " + user + "\tmention = " + mentions.getScreenName() + " ]";
		
	}

	
}
