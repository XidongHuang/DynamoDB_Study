import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;

public class BatchWrite {

	private static DynamoDB dynamoDB;
	private static AmazonDynamoDBClient client;
	
	private static void init(){
		
		AWSCredentials credentials = null;
		
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/home/tony/.aws/credentials), and is in valid format.",
                    e);
		}
		
		client = new AmazonDynamoDBClient(credentials);
		Region US_WEST2 = Region.getRegion(Regions.US_WEST_2);
		client.setRegion(US_WEST2);
		dynamoDB = new DynamoDB(client);
		
	}
	
	public static void main(String[] args) {
		init();
		
		
		batchWrite();
		
		String forumTableName = "Forum";
		String threadTableName = "Thread";
		
		TableKeysAndAttributes forumTableKeysAndAttributes = 
				new TableKeysAndAttributes(forumTableName);
		forumTableKeysAndAttributes.addHashOnlyPrimaryKeys("Name", "Amazon S3","Amazon DynamoDB");
		
		TableKeysAndAttributes threadTableKeysANdAttributes = 
				new TableKeysAndAttributes(threadTableName);
		threadTableKeysANdAttributes.addHashAndRangePrimaryKeys("ForumName", "Subject", 
				"Amazon DynamoDB","DynamoDB Thread 1",
				"Amazon DynamoDB", "DynamoDB Thread2",
				"Amazon S3", "S3 Thread 1");
		BatchGetItemOutcome outcome2 = dynamoDB.batchGetItem(
				forumTableKeysAndAttributes, threadTableKeysANdAttributes);
		
		for(String tableName: outcome2.getTableItems().keySet()){
			
			System.out.println("Items in table " + tableName);
			List<Item> items = outcome2.getTableItems().get(tableName);
			for(Item item: items){
				System.out.println(item);
			}
		}
	}

	private static void batchWrite() {
		TableWriteItems forumTableWriteItems = 
				new TableWriteItems("Forum")
				.withItemsToPut(
						new Item()
						.withPrimaryKey("Name", "Amazon RDS")
						.withNumber("Threads", 0));
		
		TableWriteItems threadTableWriteItems = new TableWriteItems("Thread")
				 .withItemsToPut(
				  new Item()
				 .withPrimaryKey("ForumName","Amazon RDS","Subject","Amazon RDS Thread 1"))
				 .withHashAndRangeKeysToDelete("ForumName","Some hash attribute value",
				"Amazon S3", "Some range attribute value");
				
		BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(forumTableWriteItems, threadTableWriteItems);
		System.out.println(outcome);
	}
}
