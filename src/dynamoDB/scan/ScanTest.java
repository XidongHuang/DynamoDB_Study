package dynamoDB.scan;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class ScanTest {

	private static DynamoDB dynamoDB;
	private static AmazonDynamoDBClient client;
	
	private static String replyTableName = "Reply";
	
	
	private static void init(){
		
		AWSCredentials credentials = null;
		
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
	
	public static void main(String[] args) {
		init();
		
		ScanRequest scanRequest = new ScanRequest()
				.withTableName(replyTableName);
		
		ScanResult result = client.scan(scanRequest);
		
		List<Map<String, AttributeValue>> results=  result.getItems();
		
		Iterator items = results.iterator();
		
		while(items.hasNext()){
			System.out.println(items.next());
			
		}
		
		System.out.println(results);
		
	}
	
}

