package dynamoDB.scan;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class ScanLimitly {

	
	
	public static void main(String[] args) {
		
		Initial.init();
		AmazonDynamoDBClient client = Initial.getClient();
		
		Map<String, AttributeValue> lastKeyEvaluated = null;
		
		do{
			ScanRequest scanRequest =
					new ScanRequest()
					.withTableName("ProductCatalog")
					.withLimit(3)
					.withExclusiveStartKey(lastKeyEvaluated)
					;
			
			ScanResult result = client.scan(scanRequest);
			
			for(Map<String, AttributeValue> item : result.getItems()){
				System.out.println(item);
			}
			lastKeyEvaluated = result.getLastEvaluatedKey();
			System.out.println("-------------");
		}while(lastKeyEvaluated != null);
		
	}
	
}
