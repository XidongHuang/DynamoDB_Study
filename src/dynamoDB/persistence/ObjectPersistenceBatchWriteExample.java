package dynamoDB.persistence;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import dynamoDB.scan.Initial;

public class ObjectPersistenceBatchWriteExample {

	static AmazonDynamoDBClient client;
	static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	
	public static void main(String [] args) throws Exception {
		Initial.init();
		client = Initial.getClient();
		try {
			DynamoDBMapper mapper = new DynamoDBMapper(client);
			
			testBatchSave(mapper);
			testBatchDelete(mapper);
			testBatchWrite(mapper);
			
			System.out.println("Example complete!");
			
			
		} catch (Throwable t) {
			System.err.println("Error running the ObjectPersistenceBatchWriteExample: " + t);
			t.printStackTrace();
		}
	}
	

	private static void testBatchSave(DynamoDBMapper mapper) {
		
		Book book1 = new Book();
		book1.id = 901;
		book1.inPublication = true;
		book1.ISBN = "902-11-11-1111";
		book1.pageCount = 100;
		book1.price = 10;
		book1.productCategory = "Book";
		book1.title = "My book created in batch wirte";
		
		Book book2 = new Book();
		book2.id = 902;
		book2.inPublication = true;
		book2.ISBN = "902-11-12-1111";
		book2.pageCount = 200;
		book2.price = 20;
		book2.productCategory = "Book";
		book2.title = "My second book created in batch wirte";
		
		
		Book book3 = new Book();
		book3.id = 903;
		book3.inPublication = true;
		book3.ISBN = "902-11-13-1111";
		book3.pageCount = 300;
		book3.price = 25;
		book3.productCategory = "Book";
		book3.title = "My third book created in batch wirte";
		
		
		System.out.println("Adding three books to ProductCatalog table.");
		mapper.batchDelete(Arrays.asList(book1, book2, book3));
		mapper.batchSave(Arrays.asList(book1, book2, book3));
		
	}
	
	private static void testBatchDelete(DynamoDBMapper mapper) {
		
		Book book1 = mapper.load(Book.class, 901);
		Book book2 = mapper.load(Book.class, 902);
		System.out.println("Deleting two books from the ProductCatalog table.");
		mapper.batchDelete(Arrays.asList(book1, book2));
	}
	
	private static void testBatchWrite(DynamoDBMapper mapper){
		
		Forum forumItem = new Forum();
		forumItem.name = "Test BatchWrite Forum";
		forumItem.threads = 0;
		forumItem.category = "Amazon Web Services";
		
		
		Thread threadItem = new Thread();
		threadItem.forumName = "AmazonDynamoDB";
		threadItem.subject = "My sample question";
		threadItem.message = "BatchWrite message";
		List<String> tags = new ArrayList<String>();
		tags.add("batch operations");
		tags.add("write");
		threadItem.tags = new HashSet<String>(tags);
		
		Book book3 = mapper.load(Book.class, 903);
		
		List<Object> objectsToWrite = Arrays.asList(forumItem, threadItem);
		List<Book> objectsToDelete = Arrays.asList(book3);
		
		DynamoDBMapperConfig config = new DynamoDBMapperConfig(DynamoDBMapperConfig.SaveBehavior.CLOBBER);
		mapper.batchWrite(objectsToWrite, objectsToDelete);
		
	}
	
	@DynamoDBTable(tableName="ProductCatalog")
	public static class Book{
		private int id;
		private String title;
		private String ISBN;
		private int price;
		private int pageCount;
		private String productCategory;
		private boolean inPublication;
		
		@DynamoDBHashKey(attributeName="Id")
		public int getId(){return id;}
		public void setId(int id) {this.id = id;}
		
		
		@DynamoDBAttribute(attributeName="Title")
		public String getTitle(){return title;}
		public void setTitle(String title){this.title = title;}
		
		@DynamoDBAttribute(attributeName="ISBN")
		public String getISBN(){return ISBN;}
		public void setISBN(String ISBN){this.ISBN = ISBN;}
		
		@DynamoDBAttribute(attributeName="Price")
		public int getPrice(){return price;}
		public void setPrice(int price) {this.price = price;}
		
		@DynamoDBAttribute(attributeName="PageCount")
		public int getPageCount() {return pageCount;}
		public void getPageCount(int pageCount) {this.pageCount = pageCount;}
		
		@DynamoDBAttribute(attributeName="ProductCategory")
		public String getProductCategory() {return productCategory;}
		public void setProductCategory(String productCategory) {this.productCategory = productCategory;}
		
		@DynamoDBAttribute(attributeName="InPublication")
		public boolean getInPublication(){return inPublication;}
		public void setInPublication(boolean inPublication) {this.inPublication = inPublication;}
		
		
		@Override
		public String toString(){
			return "Book [ISBN=" + ISBN +", price=" + price
					+ ", product category=" + productCategory + ", id="+ id
					+ ", title=" + title +	"]";
			
			
		}
		
	}
	
	@DynamoDBTable(tableName="Reply")
	public static class Reply{
		private String id;
		private String replyDateTime;
		private String message;
		private String postedBy;
		
		@DynamoDBHashKey(attributeName="Id")
		public String getId() {return id;}
		public void setId(String id) {this.id = id;}
		
		@DynamoDBRangeKey(attributeName="ReplyDateTime")
		public String getReplyDateTime() {return replyDateTime;}
		public void setReplyDateTime(String replyDateTime) {this.replyDateTime = replyDateTime;}
		
		@DynamoDBAttribute(attributeName="Message")
		public String getMessage(){return message;}
		public void setMessage(String message) {this.message = message;}
		
		@DynamoDBAttribute(attributeName="PostedBy")
		public String getPostedBy(){return postedBy;}
		public void setPostedBy(String postedBy){this.postedBy = postedBy;}
		
	}
	
	@DynamoDBTable(tableName="Thread")
	public static class Thread{
		private String forumName;
		private String subject;
		private String message;
		private String lastPostedDateTime;
		private String lastPostedBy;
		private Set<String> tags;
		private int answered;
		private int views;
		private int replies;
		
		
		@DynamoDBHashKey(attributeName="ForumName")
		public String getFourmName() {return forumName;}
		public void setForumName(String forumName) {this.forumName = forumName;}
		
		@DynamoDBRangeKey(attributeName="Subject")
		public String getSubject() {return subject;}
		public void setSubject(String subject){this.subject = subject;}
		
		@DynamoDBAttribute(attributeName="Message")
		public String getMessage(){return message;}
		public void setMessage(String message){this.message = message;}
		
		@DynamoDBAttribute(attributeName="LastPostedDateTime")
		public String getLastPostedDateTime(){return lastPostedDateTime;}
		public void setLastPostedDateTime(String lastPostedDateTime){this.lastPostedDateTime = lastPostedDateTime;}
		
		@DynamoDBAttribute(attributeName="LastPostedBy")
		public String getLastPostedBy() { return lastPostedBy;}
		public void setLastPostedBy(String lastPostedBy){this.lastPostedBy = lastPostedBy;}
		
		@DynamoDBAttribute(attributeName="Tags")
		public Set<String> getTags() {return tags;}
		public void setTags(Set<String> tags){this.tags= tags;}
		
		@DynamoDBAttribute(attributeName="Answered")
		public int getAnswered(){return answered;}
		public void setAnswered(int answered){this.answered = answered;}
		
		@DynamoDBAttribute(attributeName="Views")
		public int getViews() {return views;}
		public void setViews(int views){this.views = views;}
		
		@DynamoDBAttribute(attributeName="Replies")
		public int getReplies() {return replies;}
		public void setReplies(int replies) {this.replies = replies;}
	}
	
	@DynamoDBTable(tableName="Forum")
	public static class Forum{
		private String name;
		private String category;
		private int threads;
		
		@DynamoDBHashKey(attributeName="Name")
		public String getName() {return name;}
		public void setName(String name) {this.name = name;}
		
		@DynamoDBAttribute(attributeName="Category")
		public String getCategory(){return category;}
		public void setCategory(String category){this.category = category;}
		
		@DynamoDBAttribute(attributeName="Thread")
		public int getThreads() {return threads;}
		public void setThreads(int threads) {this.threads = threads;}
	}
	
}
