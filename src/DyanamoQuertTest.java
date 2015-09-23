
import javax.mail.event.StoreEvent;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;

public class DyanamoQuertTest {

	private static DynamoDB dynamoDB;
	private static AmazonDynamoDBClient client;

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
		Region us_WEST2 = Region.getRegion(Regions.US_WEST_2);
		client.setRegion(us_WEST2);
		dynamoDB = new DynamoDB(client);

	}
	
	public static void main(String[] args) {
		init();
		
		Table table = dynamoDB.getTable("ProductCatalog");
		
		Item item = table.getItem("Id", 101);
		System.out.println(item.toJSONPretty());
		System.out.println("------------------");
		
		GetItemSpec spec = new GetItemSpec()
				.withPrimaryKey("Id", 206)
				.withProjectionExpression("Id, Title, RelatedItems[0], Reviews.FiveStar")
				.withConsistentRead(true);
		
		Item item2 = table.getItem(spec);
		
		System.out.println(item2.toJSONPretty());
		//storeVendor(table);
		System.out.println("------------------");
		
		
		GetItemSpec spec2 = new GetItemSpec()
				.withPrimaryKey("Id", 210);
		
		System.out.println("All vendor info:");
		spec2.withProjectionExpression("VendorInfo");
		System.out.println(table.getItem(spec2).toJSONPretty());
		System.out.println("-----------------------");
		System.out.println("A single vendor:");
		spec2.withProjectionExpression("VendorInfo.V03");
		System.out.println(table.getItem(spec2).toJSONPretty());
		System.out.println("-----------------------");
		System.out.println("First office location for this vendor:");
		spec2.withProjectionExpression("VendorInfo.V03.Offices[0]");
		System.out.println(table.getItem(spec2).toJSONPretty());
		
	}
	

	private static void storeVendor(Table table){
		
		String vendorDocument = "{"
				 + " \"V01\": {"
				 + " \"Name\": \"Acme Books\","
				 + " \"Offices\": [ \"Seattle\" ]"
				 + " },"
				 + " \"V02\": {"
				 + " \"Name\": \"New Publishers, Inc.\","
				 + " \"Offices\": [ \"London\", \"New York\"" + "]" + "},"
				 + " \"V03\": {"
				 + " \"Name\": \"Better Buy Books\","
				 + "\"Offices\": [ \"Tokyo\", \"Los Angeles\", \"Sydney\""
				 + " ]"
				 + " }"
				 + " }";
				Item item = new Item()
				 .withPrimaryKey("Id", 210)
				 .withString("Title", "Book 210 Title")
				 .withString("ISBN", "210-2102102102")
				 .withNumber("Price", 30)
				 .withJSON("VendorInfo", vendorDocument);
				PutItemOutcome outcome = table.putItem(item);
	}
	

}
