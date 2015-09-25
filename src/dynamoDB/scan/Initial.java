package dynamoDB.scan;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

public class Initial {

	private static DynamoDB dynamoDB;
	private static AmazonDynamoDBClient client;
	
	
	
	

	public static void init() {
		
		AWSCredentials credentials;
		
		try {
			
			credentials = new ProfileCredentialsProvider("default").getCredentials();
			
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (/home/tony/.aws/credentials), and is in valid format.", e);
		}
		client = new AmazonDynamoDBClient(credentials);
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		client.setRegion(usWest2);
		dynamoDB = new DynamoDB(client);
		
		
	}
	
	public static DynamoDB getDynamoDB(){
		return dynamoDB;
	}
	
	public static AmazonDynamoDBClient getClient(){
		return client;
	}
	
}
