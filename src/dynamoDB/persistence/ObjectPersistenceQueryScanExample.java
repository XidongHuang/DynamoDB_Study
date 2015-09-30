package dynamoDB.persistence;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;

import dynamoDB.persistence.ObjectPersistenceBatchWriteExample.Reply;
import dynamoDB.query.Initial;

public class ObjectPersistenceQueryScanExample {

	private static AmazonDynamoDBClient client;
	
	
	public static void main(String[] args) {
		Initial.init();
		
		client = Initial.getClient();
		
		
		try {
			DynamoDBMapper mapper = new DynamoDBMapper(client);
			
			GetBook(mapper, 101);
			
			String forumName = "Amazon DynamoDB";
			String threadSubject = "DynamoDB Thread 1";
			
			FindRepliesInLast15Days(mapper, forumName, threadSubject);
			FindRepliesPostedWithinTimePeriod(mapper, forumName, threadSubject);
			FindBooksPricedLessThanSpecifiedValue(mapper, "20");
			
			int numberOfThreads = 16;
			FindBicyclesOfSpecificTypeWithMultipleThreads(mapper, numberOfThreads,"Road");
			
			System.out.println("Excample complete!");
			
			
		} catch (Throwable t) {
			System.err.println("Error running the ObjectPersistenceQueryScanExample: " + t);
			t.printStackTrace();
		
		}
		
		
		
	}
	
	
	private static void GetBook(DynamoDBMapper mapper, int id) throws Exception{
		System.out.println("GetBook: Get book Id = '101' ");
		System.out.println("Book table has no range key attribute, so you Get (but no query).");
		
		Book book = mapper.load(Book.class, 101);
		System.out.format("Id = %s Title = %s, ISBN = %s %n", book.getId(), book.getTitle(), book.getISBN());
	}
	
	private static void FindRepliesInLast15Days(DynamoDBMapper mapper, String forumName, String threadSubject) throws Exception{
		System.out.println("FindRepliesInLast15Days: Replies within last 15 days.");
		String hashKey = forumName + "#"+ threadSubject;
		
		long twoWeeksAgoMilli = (new Date()).getTime() -  (300L * 24L * 60L * 60L*1000L);
		
		Date twoWeeksAgo = new Date();
		
		twoWeeksAgo.setTime(twoWeeksAgoMilli);
		SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
		String twoWeeksAgoStr = dateFormatter.format(twoWeeksAgo);
		
		Condition rangeKeyCondition = new Condition()
				.withComparisonOperator(ComparisonOperator.GT.toString())
				.withAttributeValueList(new AttributeValue().withS(twoWeeksAgoStr.toString()));
		
		Reply replyKey = new Reply();
		replyKey.setId(hashKey);
		
		DynamoDBQueryExpression<Reply> queryExpression = new DynamoDBQueryExpression<Reply>()
				.withHashKeyValues(replyKey)
				.withRangeKeyCondition("ReplyDateTime", rangeKeyCondition);
		
		List<Reply> latestReplies = mapper.query(Reply.class, queryExpression);
		
		
		for(Reply reply:latestReplies){
			
			System.out.format("Id = %s, Message=%s, PostedBy = %s %n, ReplyDateTime=%s %n",
					reply.getId(), reply.getMessage(), reply.getPostedBy(), reply.getReplyDateTime());
		}
		
		
		
	}
	
	
	private static void FindRepliesPostedWithinTimePeriod(DynamoDBMapper mapper, String forumName, String threadSubject) throws Exception{
		
		String hashKey = forumName + "#" + threadSubject;
		System.out.println("FindRepliesPostedWithinTimePeriod: Find replies for thread Messge = 'DynamoDB Thread 2' "
				+ "posted within a period.");
		long startDateMilli = (new Date()).getTime() - (50L * 24L* 60L* 60L * 1000L);
		long endDateMilli = (new Date()).getTime() - (35L *  24L* 60L* 60L * 1000L);
		SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		
		dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
		String starDate = dateFormatter.format(startDateMilli);
		String endDate = dateFormatter.format(endDateMilli);
		
		Condition rangeKeyCondition = new Condition()
				.withComparisonOperator(ComparisonOperator.BETWEEN.toString())
				.withAttributeValueList(new AttributeValue().withS(starDate),
						new AttributeValue().withS(endDate));
		
		Reply replyKey = new Reply();
		replyKey.setId(hashKey);
		
		DynamoDBQueryExpression<Reply> queryExpression = new DynamoDBQueryExpression<Reply>()
				.withHashKeyValues(replyKey)
				.withRangeKeyCondition("ReplyDateTime", rangeKeyCondition);
				
		List<Reply> betweenReplies = mapper.query(Reply.class, queryExpression);
		
		for(Reply reply: betweenReplies){
			System.out.format("Id = %s, Message= %s, PostedBy = %s %n, PostedDateTime=%s %n",
					reply.getId(), reply.getMessage(), reply.getPostedBy(), reply.getReplyDateTime());
			
			
		}
				
		
	}
	
	private static void FindBooksPricedLessThanSpecifiedValue(DynamoDBMapper mapper, String value) throws Exception{
		
		System.out.println("FindBooksPricedLessTHanSpecifiedValue: Scan Product Catalog.");
		
		DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
		scanExpression.addFilterCondition("Price", 
				new Condition()
				.withComparisonOperator(ComparisonOperator.LT)
				.withAttributeValueList(new AttributeValue().withN(value)));
		
		scanExpression.addFilterCondition("ProductCategory", 
				new Condition().withComparisonOperator(ComparisonOperator.EQ)
				.withAttributeValueList(new AttributeValue().withS("Book")));
		
		List<Book> scanResult = mapper.scan(Book.class, scanExpression);
		
		for(Book book:scanResult){
			System.out.println(book);
		}
		
		
	}
	
	
	private static void FindBicyclesOfSpecificTypeWithMultipleThreads(DynamoDBMapper mapper,
			int numberOfThreads, String bicycleType) throws Exception{
		
		System.out.println("FindBicycleOfSpecificTypeWithMultipleThreads: Scan ProductCatalog With Multiple Threads.");
		DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
		scanExpression.addFilterCondition("ProductCategory", 
				new Condition().withComparisonOperator(ComparisonOperator.EQ)
				.withAttributeValueList(new AttributeValue().withS("Bicycle")));
		
		scanExpression.addFilterCondition("BicycleType", new Condition()
				.withComparisonOperator(ComparisonOperator.EQ)
				.withAttributeValueList(new AttributeValue().withS(bicycleType)));
		List<Bicycle> scanResult = mapper.parallelScan(Bicycle.class, scanExpression, numberOfThreads);
		for(Bicycle bicycle: scanResult){
			System.out.println(bicycle);
		}
		
	}
	
	
	@DynamoDBTable(tableName="ProductCatalog")
	public static class Bicycle{
		private int id;
		private String title;
		private String description;
		private String bicycleType;
		private String brand;
		private int price;
		private String gender;
		private Set<String> color;
		private String productCategory;
		
		@DynamoDBHashKey(attributeName="Id")
		public int getId() {return id;}
		public void setId(int id) {this.id = id;}

		@DynamoDBAttribute(attributeName="Title")
		public String getTitle() {return title;}
		public void setTitle(String title) {this.title = title;}

		
		@DynamoDBAttribute(attributeName="Description")
		public String getDescription() {return description;}
		public void setDescription(String description) {this.description = description;}

		
		@DynamoDBAttribute(attributeName="BicycleType")
		public String getBicycleType() {return bicycleType;}
		public void setBicycleType(String bicycleType) {this.bicycleType = bicycleType;}

		
		@DynamoDBAttribute(attributeName="Brand")
		public String getBrand() {return brand;}
		public void setBrand(String brand) {this.brand = brand;}

		
		@DynamoDBAttribute(attributeName="Price")
		public int getPrice() {return price;}
		public void setPrice(int price) {this.price = price;}
		
		@DynamoDBAttribute(attributeName="Gender")
		public String getGender() {return gender;}
		public void setGender(String gender) {this.gender = gender;}
		
		@DynamoDBAttribute(attributeName="Color")
		public Set<String> getColor() {	return color;}
		public void setColor(Set<String> color) {this.color = color;}
		
		
		@DynamoDBAttribute(attributeName="ProductCategory")
		public String getProductCategory() {return productCategory;}
		public void setProductCategory(String productCategory) {this.productCategory = productCategory;}
		
		
		
		
		
	}
	
	
	@DynamoDBTable(tableName = "ProductCatalog")
	public static class Book {

		private int id;
		private String title;
		private String ISBN;
		private int price;
		private int pageCount;
		private String productCategory;
		private boolean inPublication;

		@DynamoDBHashKey(attributeName="Id")
		public int getId() {return id;}
		public void setId(int id) {this.id = id;}
		
		@DynamoDBAttribute(attributeName="Title")
		public String getTitle() {return title;}
		public void setTitle(String title) {this.title = title;}

		@DynamoDBAttribute(attributeName="ISBN")
		public String getISBN() {return ISBN;}
		public void setISBN(String iSBN) {ISBN = iSBN;}

		@DynamoDBAttribute(attributeName="Price")
		public int getPrice() {return price;}
		public void setPrice(int price) {this.price = price;}

		@DynamoDBAttribute(attributeName="PageCount")
		public int getPageCount() {return pageCount;}
		public void setPageCount(int pageCount) {this.pageCount = pageCount;}

		@DynamoDBAttribute(attributeName="ProductCategory")
		public String getProductCategory() {return productCategory;}
		public void setProductCategory(String productCategory) {this.productCategory = productCategory;}

		@DynamoDBAttribute(attributeName="InPublication")
		public boolean isInPublication() {return inPublication;}
		public void setInPublication(boolean inPublication) {this.inPublication = inPublication;}

		@Override
		public String toString() {
			return "Book [id=" + id + ", title=" + title + ", ISBN=" + ISBN 
					+ ", price=" + price + ", pageCount=" + pageCount 
					+ ", productCategory=" + productCategory + ", inPublication=" + inPublication + "]";
		}

	}
	
}
