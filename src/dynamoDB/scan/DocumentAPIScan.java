package dynamoDB.scan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class DocumentAPIScan {

	public static void main(String[] args) {
		Initial.init();
		AmazonDynamoDBClient client = Initial.getClient();
		DynamoDB dynamoDB = Initial.getDynamoDB();
		
		findProductsForPriceLessThanZero(dynamoDB, client);
		
		
	}
	
	private static void findProductsForPriceLessThanZero(DynamoDB dynamoDB, AmazonDynamoDBClient client){
		
		Table table = dynamoDB.getTable("ProductCatalog");
		
		Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
		expressionAttributeValues.put(":pr", new AttributeValue().withN("100"));
		
//		ItemCollection<ScanOutcome> items = table.scan(
//				"Price < :pr",
//				"Id, Title, ProductCategory, Price",
//				null,
//				expressionAttributeValues
//				);
//		
////		System.out.println("Scan of ProductCatalog for items with a price less than 100." );
////		Iterator<Item> iterator = items.iterator();
////		while(iterator.hasNext()){
////			System.out.println(iterator.next().toJSONPretty());
////		}
		
		System.out.println("------------------");
		ScanRequest scanRequest = new ScanRequest()
				.withTableName("ProductCatalog")
				.withFilterExpression("Price < :pr")
				.withProjectionExpression("Id, Title, ProductCategory, Price")
				.withExpressionAttributeValues(expressionAttributeValues);
		
		ScanResult result = client.scan(scanRequest);
		List<Map<String, AttributeValue>> results = result.getItems();
		
		Iterator iterator = results.iterator();
		
		while(iterator.hasNext()){
			System.out.println(iterator.next());
		}
		
	}
	
	
	
	
}
