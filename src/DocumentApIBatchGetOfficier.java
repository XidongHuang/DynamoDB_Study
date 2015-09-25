import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;

public class DocumentApIBatchGetOfficier {

	static DynamoDB dynamoDB;
	static AmazonDynamoDBClient client;

	static String forumTableName = "Forum";
	static String threadTableName = "Thread";

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
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		client.setRegion(usWest2);
		dynamoDB = new DynamoDB(client);

	}

	public static void main(String[] args) throws IOException {
		init();

		retrieveMultipleItemsBatchGet();

	}

	private static void retrieveMultipleItemsBatchGet() {

		try {

			TableKeysAndAttributes forumTableKeysAndAttributes = new TableKeysAndAttributes(forumTableName);
			forumTableKeysAndAttributes.addHashOnlyPrimaryKeys("Name", "Amazon S3", "Amazon DynamoDB");

			TableKeysAndAttributes threadTableKeysAndAttributes = new TableKeysAndAttributes(threadTableName);
			threadTableKeysAndAttributes.addHashAndRangePrimaryKeys("ForumName", "Subject", "Amazon DynamoDB",
					"DynamoDB Thread 1", "Amazon DynamoDB", "DynamoDB Thread 2", "Amazon S3", "S3 Thread 1");

			Map<String, TableKeysAndAttributes> requestItems = new HashMap<String, TableKeysAndAttributes>();
			requestItems.put(forumTableName, forumTableKeysAndAttributes);
			requestItems.put(threadTableName, threadTableKeysAndAttributes);

			System.out.println("Making the request.");

			BatchGetItemOutcome outcome = dynamoDB.batchGetItem(forumTableKeysAndAttributes,
					threadTableKeysAndAttributes);

			do {

				for (String tableName : outcome.getTableItems().keySet()) {
					System.out.println("Items in table " + tableName);
					List<Item> items = outcome.getTableItems().get(tableName);
					for (Item item : items) {
						System.out.println(item.toJSONPretty());
					}
				}

				// Check for unprocessed keys which could happen if you exceed
				// provisioned
				// throughput or reach the limit on response size.

				Map<String, KeysAndAttributes> unprocessedKeys = outcome.getUnprocessedKeys();

				if (outcome.getUnprocessedKeys().size() == 0) {
					System.out.println("No unprocessed keys found");
				} else {
					System.out.println("Retrieving the unprocessed keys");
					outcome = dynamoDB.batchGetItemUnprocessed(unprocessedKeys);
				}

			} while (outcome.getUnprocessedKeys().size() > 0);

		} catch (Exception e) {
			System.err.println("Failed to retrieve items.");
			System.err.println(e.getMessage());
		}

	}

}