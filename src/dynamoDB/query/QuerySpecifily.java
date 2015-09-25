package dynamoDB.query;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

public class QuerySpecifily {

	public static void main(String[] args) {
		Initial.init();
		DynamoDB dynamoDB = Initial.getDynamoDB();
		
		Table table = dynamoDB.getTable("Reply");
		
		
		long twoWeeksAgoMilli = 
				(new Date()).getTime() - (300L * 24L * 60L * 60L * 1000L);
		Date twoWeekAgo = new Date();
		twoWeekAgo.setTime(twoWeeksAgoMilli);
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		String twoWeeksAgoStr = df.format(twoWeekAgo);
		
		QuerySpec spec = new QuerySpec()
				.withKeyConditionExpression("Id = :v_id ")
//				.withFilterExpression("PostedBy = :v_posted_by")
				.withValueMap(new ValueMap()
						.withString(":v_id", "Amazon DynamoDB#DynamoDB Thread 1"))
//						.withString(":v_reply_dt_tm", twoWeeksAgoStr)
//						.withString(":v_posted_by", "User B"))
				.withConsistentRead(true);
		spec.withMaxPageSize(1);
		
		
		
		ItemCollection<QueryOutcome> items = table.query(spec);
//		Iterator<Item> iterator = items.iterator();
//		
//		while(iterator.hasNext()){
//			System.out.println(iterator.next().toJSONPretty());
//			
//		}
		
		
		int pageNum = 0;
		for(Page<Item, QueryOutcome> page:items.pages()){
			
			System.out.println("\nPage: " + ++pageNum);
			
			Iterator<Item> item = page.iterator();
			while(item.hasNext()){
				System.out.println(item.next().toJSONPretty());
			}
		}
		
		
		
	}
	
	
}
