package TweetReader;

import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class Main {

  // TWITTER SETTINGS
	private static final String USER = "";
	private final static int TWEETQTY = 100;
	private static final String STARTFROM = "2013-01-01";

	// MONGODB SETTINGS
	private final static int DBPORT = 27017;

	private final static String DBUSER = "admin";
	private final static char[] DBPASSWORD = "admin".toCharArray();
	private final static String DBNAME = "MyTweetDB";
	private static final String DBHOST = "localhost";
	private final static long COLLECTIONSIZE = 2000000000l;

	public static void main(String[] args) {
		DBCollection collection = null;
		BasicDBObject obj;
		String location, sender;
		String hour;

		// The Following three statistics will be collected from the 100 tweets.
		// The properties object is ideal in keeping the key unique and the
		// value will represent a count.
		// For example, if two tweets were from canada and 1 from US, the
		// globalLocations should have { {Canada, 2}, {US, 1}}.
		// If three tweets were from AbdulMajid then, usersCount should have
		// {AbdulMajid, 2}

		Properties globalLocations = new Properties();
		Properties usersCount = new Properties();
		Properties hourlyTweets = new Properties();

		// Attempt to connect with MongoDB
		try {
			MongoClient mongoClient = new MongoClient(DBHOST, DBPORT);
			DB db = mongoClient.getDB(DBNAME);
			boolean auth = db.authenticate(DBUSER, DBPASSWORD);
			if (auth) {
				DBObject options = BasicDBObjectBuilder.start()
						.add("capped", true).add("size", COLLECTIONSIZE).get();
				collection = db.createCollection(USER, options);
			}
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}

		Twitter twitter = new TwitterFactory().getInstance();
		// Setup the query for the Tweet using the variables set above.
		Query query = new Query("from:" + USER);
		query.setCount(TWEETQTY);
		query.setSince(STARTFROM);

		// Attempt to query the twitter..
		try {
			QueryResult result = twitter.search(query);

			List<Status> tweetResults = result.getTweets();

			// Print the results on the console and place the results in the
			// MongoDB collection
			for (Status status : tweetResults) {
				sender = status.getSource();
				location = status.getPlace().getFullName();
				Date d = status.getCreatedAt();
				Calendar cal = Calendar.getInstance();
				cal.setTime(d);
				hour = String.valueOf(cal.HOUR_OF_DAY);
				System.out.println("Text : " + status.getText() + "From: "
						+ sender + "Time: " + status.getCreatedAt());

				obj = new BasicDBObject();
				obj.put("From", sender);
				obj.put("Text", status.getText());
				obj.put("Time", status.getCreatedAt());
				obj.put("Location", location);
				obj.put("Retweet Count", status.getRetweetCount());
				collection.insert(obj);

				// update statistics now
				int num = 1;
				if (globalLocations.contains(location)) {
					num = Integer
							.valueOf(globalLocations.getProperty(location));
					num++;
					globalLocations
							.setProperty(location, (String.valueOf(num)));
				} else {
					globalLocations
							.setProperty(location, (String.valueOf(num)));
				}
				num = 1;
				if (usersCount.contains(sender)) {
					num = Integer.valueOf(usersCount.getProperty(sender));
					num++;
					usersCount.setProperty(sender, (String.valueOf(num)));
				} else {
					usersCount.setProperty(sender, (String.valueOf(num)));
				}
				num = 1;
				if (hourlyTweets.contains(hour)) {
					num = Integer.valueOf(hourlyTweets.getProperty(sender));
					num++;
					hourlyTweets.setProperty(hour, (String.valueOf(num)));
				} else {
					hourlyTweets.setProperty(hour, (String.valueOf(num)));
				}

			}
		} catch (TwitterException e) {
			e.printStackTrace();
		}
		//print statistics
		System.out.println(usersCount);
		System.out.println(hourlyTweets);
		System.out.println(globalLocations);
	}
}
