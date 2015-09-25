import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

public class DocumentAPIBatchWrite {

	private static DynamoDB dynamoDB;
	private static AmazonDynamoDBClient client;

	
	private static String forumTable = "Forum";
	private static String threadTableName = "Thread";
	
	private static void init() {

		AWSCredentials credentials = null;

		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (/home/tony/.aws/credentials), and is in valid format.", e);
		}
		
		
		
		client = new AmazonDynamoDBClient(credentials);
		Region US_WEST2 = Region.getRegion(Regions.US_WEST_2);
		client.setRegion(US_WEST2);
		dynamoDB = new DynamoDB(client);
		
	}
	
	
	public static void main(String[] args) {
		init();
		
		writeMultipleItemsBatchWrite();
		
		
	}
	
	private static void writeMultipleItemsBatchWrite(){
		
		try {
			
			TableWriteItems forumTableWriteItems = 
					new TableWriteItems(forumTable)
					.withItemsToPut(new Item()
							.withPrimaryKey("Name", "Amazon RDS")
							.withNumber("Thread", 0));
			
			TableWriteItems threadTableWriteItems =
					new TableWriteItems(threadTableName)
					.withItemsToPut(new Item()
							.withPrimaryKey("ForumName", "Amazon RDS",
									"Subject", "Amazon RDS Thread 1")
							.withString("Message", "ElastiCache Thread 1 message")
							.withStringSet("Tags", new HashSet<String>(
									Arrays.asList("cache","in-memory"))))
					.withHashAndRangeKeysToDelete("ForumName", "Subject", "Amazon S3",
							"S3 Thread 100");
			System.out.println("Making the request.");
			BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(forumTableWriteItems,
					threadTableWriteItems);
			
			do{
				
				Map<String, List<WriteRequest>> unprocessedItems =
						outcome.getUnprocessedItems();
				if(outcome.getUnprocessedItems().size()==0){
					System.out.println("No unprocessed items found");
				} else {
					System.out.println("Retrieving the unprocessed items");
					outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
				}
				
			} while(outcome.getUnprocessedItems().size() > 0);
			
			
			
		} catch (Exception e) {

			System.err.println("Failed to retrieve items: ");
			e.printStackTrace(System.err);
		}
		
	}
	

}
