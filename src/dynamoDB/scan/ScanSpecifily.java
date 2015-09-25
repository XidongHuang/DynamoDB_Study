package dynamoDB.scan;

import java.util.HashMap;
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

public class ScanSpecifily {

	private static DynamoDB dynamoDB;
	private static AmazonDynamoDBClient client;
	
	private static String tableName = "ProductCatalog";
	
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
		
		Map<String, AttributeValue> expressionAttributeValues = 
				new HashMap<String, AttributeValue>();
		expressionAttributeValues.put(":val", new AttributeValue().withN("100"));
		
		ScanRequest scanRequest =  new ScanRequest()
				.withTableName(tableName)
				.withFilterExpression("Price < :val")
				.withProjectionExpression("Id")
				.withExpressionAttributeValues(expressionAttributeValues);
		
		ScanResult result = client.scan(scanRequest);
		for(Map<String, AttributeValue> item: result.getItems()){
			System.out.println(item);
		}
		
	}
	
	
	
}
