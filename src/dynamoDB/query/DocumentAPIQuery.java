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

public class DocumentAPIQuery {

	private static String tableName = "Reply";
	
	
	private static DynamoDB dynamoDB;
	

	public static void main(String[] args) {
		Initial.init();
		dynamoDB = Initial.getDynamoDB();
		
		String forumName = "Amazon DynamoDB";
		String threadSubject = "DynamoDB Thread 1";
		
//		findRepliesForAThread(forumName, threadSubject);
//		findRepliesForAThreadSpecifyOptionalLimit(forumName, threadSubject);
//		findRepliesInLast15DaysWithConfig(forumName, threadSubject);
//		findRepliesPostedWithinTimePeriod(forumName, threadSubject);
		findRepliesUsingAFilterExpression(forumName, threadSubject);
		
	}
	
	private static void findRepliesPostedWithinTimePeriod(String forumName, String threadSubject) {

		Table table = dynamoDB.getTable(tableName);
		String replyId = forumName +"#"+ threadSubject;
		
		long startDateMilli = (new Date()).getTime() -(300L*24L*60L*60L*1000L);
		long endDateMilli = (new Date()).getTime() - (5L*24L*60L*60L*1000L);
		
		java.text.SimpleDateFormat df = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		
		String startDate = df.format(startDateMilli);
		String endDate = df.format(endDateMilli);
		
		QuerySpec spec = new QuerySpec()
				.withProjectionExpression("Message, ReplyDateTime, PostedBy")
				.withKeyConditionExpression("Id = :v_id and ReplyDateTime between"
						+ " :v_start_dt and :v_end_dt")
				.withValueMap(new ValueMap()
						.withString(":v_id", replyId)
						.withString(":v_start_dt", startDate)
						.withString(":v_end_dt", endDate));
		ItemCollection<QueryOutcome> items = table.query(spec);
		
		System.out.println("\nfindRepliesPostedWithinTimePeriod's result:");
		Iterator<Item> iterator = items.iterator();
		while(iterator.hasNext()){
			System.out.println(iterator.next().toJSONPretty());
		}
		
		
		
		
	}

	private static void findRepliesForAThread(String forumName, String threadSubject){
		
		Table table = dynamoDB.getTable(tableName);
		
		String replyId = forumName +"#"+ threadSubject;
		
		QuerySpec spec = new QuerySpec()
				.withKeyConditionExpression("Id = :v_id")
				.withValueMap(new ValueMap()
						.withString(":v_id", replyId));
		
		ItemCollection<QueryOutcome> items = table.query(spec);
		
		System.out.println("\nfindRepliesForAThread results:");
		
		Iterator<Item> iterator = items.iterator();
		while(iterator.hasNext()){
			
			System.out.println(iterator.next().toJSONPretty());
		}
		
		
	}
	
	private static void findRepliesForAThreadSpecifyOptionalLimit(String forumName, String threadSuject){
		
		Table table = dynamoDB.getTable(tableName);
		
		String replyId = forumName +"#"+threadSuject;
		
		QuerySpec spec = new QuerySpec()
				.withKeyConditionExpression("Id = :v_id")
				.withValueMap(new ValueMap()
						.withString(":v_id",replyId))
				.withMaxPageSize(1);
		
		ItemCollection<QueryOutcome> items = table.query(spec);
		
		System.out.println("\nfindRepliesForAThreadSpecifyOptionalLimit result:");
		
		int pageNum = 0;
		for(Page<Item, QueryOutcome> page : items.pages()){
			
			System.out.println("\nPage: " + ++pageNum);
			
			Iterator<Item> item = page.iterator();
			while(item.hasNext()){
				System.out.println(item.next().toJSONPretty());
			}
			
		}
		
		
	}
	
	
	
	private static void findRepliesInLast15DaysWithConfig(String forumName, String threadSubject){
		
		Table table = dynamoDB.getTable(tableName);
		
		long twoWeeksAgoMilli = (new Date()).getTime() - (300L*24L*60L*100L);
		
		Date twoWeeksAgo = new Date();
		twoWeeksAgo.setTime(twoWeeksAgoMilli);
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		String twoWeeksAgoStr = df.format(twoWeeksAgo);
		
		String replyId = forumName+"#"+threadSubject;
		
		QuerySpec spec = new QuerySpec()
				.withProjectionExpression("Message, ReplyDateTime, PostedBy")
				.withKeyConditionExpression("Id = :v_id and ReplyDateTime <= :v_reply_dt_tm")
				.withValueMap(new ValueMap()
						.withString(":v_id", replyId)
						.withString(":v_reply_dt_tm", twoWeeksAgoStr));
		
		ItemCollection<QueryOutcome> items = table.query(spec);
		
		System.out.println("\nfindRepliesInLast15DaysWithConfig results:");
		Iterator<Item> iterator = items.iterator();
		
		while(iterator.hasNext()){
			System.out.println(iterator.next().toJSONPretty());
		}
		
		
		
	}
	
	private static void findRepliesUsingAFilterExpression(String forumName, String threadSubject){
		
		Table table = dynamoDB.getTable(tableName);
		
		String replyId = forumName+"#"+threadSubject;
		
		QuerySpec spec = new QuerySpec()
				.withProjectionExpression("Message, ReplyDateTime, PostedBy")
				.withKeyConditionExpression("Id= :v_id")
				.withFilterExpression("PostedBy =  :v_postedby")
				.withValueMap(new ValueMap()
						.withString(":v_id", replyId)
						.withString(":v_postedby", "User B"))
				.withMaxPageSize(1);
		
		ItemCollection<QueryOutcome> items = table.query(spec);
		
		System.out.println("\nfindRepliesUsingAFilterExpression's result:");
		
		int pageNum = 0;
		
		for(Page<Item, QueryOutcome> page:items.pages()){
			
			System.out.println("\nPage: " + ++pageNum);
			
			Iterator<Item> iterator = page.iterator();
			while(iterator.hasNext()){
				System.out.println(iterator.next().toJSONPretty());
			}
		}
				
		
		
	}
	
	
}
