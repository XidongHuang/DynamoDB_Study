package dynamoDB.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

public class QueryTest {

	
	public static void main(String[] args) {
		Initial.init();
		DynamoDB dynamoDB = Initial.getDynamoDB();
		
		Table table = dynamoDB.getTable("Reply");
		
		
//		Map<String, Object> valueMap = new HashMap<>();
//		valueMap.put(":v_id", "Amazon DynamoDB#DynamoDB Thread 1");
		
		QuerySpec spec = new QuerySpec()
				.withKeyConditionExpression("Id = :v_id")
				.withValueMap(new ValueMap()
						.withString(":v_id", "Amazon DynamoDB#DynamoDB Thread 1"));
		
		ItemCollection<QueryOutcome> items = table.query(spec);
		
		Iterator<Item> iterator = items.iterator();
		Item item = null;
		while(iterator.hasNext()){
			item = iterator.next();
			
			System.out.println(item.toJSONPretty());
			
		}
					
		
		
	}
	
	
}
