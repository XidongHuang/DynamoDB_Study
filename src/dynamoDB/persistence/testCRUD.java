package dynamoDB.persistence;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.amazonaws.services.cloudfront.model.UpdateDistributionRequest;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import dynamoDB.scan.Initial;

public class testCRUD {

	static AmazonDynamoDBClient client;
	
	public static void main(String[] args) {
		Initial.init();
		client = Initial.getClient();
		
		testCURDOperations();
		System.out.println("Example complete!");
		
	}
	
	@DynamoDBTable(tableName="ProductCatalog")
	public static class CatalogItem{
		private Integer id;
		private String title;
		private String ISBN;
		private Set<String> bookAuthors;
		
		@DynamoDBHashKey(attributeName="Id")
		public Integer getId() {return id;}
		public void setId(Integer id) {this.id = id;}
		
		@DynamoDBAttribute(attributeName="Title")
		public String getTitle(){return title;}
		public void setTitle(String title) {this.title = title;}
		
		@DynamoDBAttribute(attributeName="ISBN")
		public String getISBN() {return ISBN;}
		public void setISBN(String ISBN){this.ISBN = ISBN;}
		
		@DynamoDBAttribute(attributeName="Authors")
		public Set<String> getBookAuthors() {return bookAuthors;}
		public void setBookAuthors(Set<String> bookAuthors) {this.bookAuthors = bookAuthors;}
		
		
		@Override
		public String toString(){
			
			return "Book [ISBN=" + ISBN + ", bookAuthors=" + bookAuthors
					+ ", id=" + id + ", title=" + title + "]";
		}
	}
	

	private static void testCURDOperations(){
		
		CatalogItem item = new CatalogItem();
		item.setId(601);
		item.setTitle("Book 601");
		item.setISBN("611-1111111111");
		item.setBookAuthors(new HashSet<String>(Arrays.asList("Author1", "Author2")));
		
		
		DynamoDBMapper mapper = new DynamoDBMapper(client);
		mapper.delete(item);
		mapper.save(item);
		
		CatalogItem itemRetrieved = mapper.load(CatalogItem.class, 601);
		System.out.println("Item retrieved:");
		System.out.println(itemRetrieved);
		
		
		itemRetrieved.setISBN("622-2222222222");
		itemRetrieved.setBookAuthors(new HashSet<String>(Arrays.asList("Author1", "Author3")));
		mapper.save(itemRetrieved);
		System.out.println("Item updated:");
		System.out.println(itemRetrieved);
		

		DynamoDBMapperConfig config = new DynamoDBMapperConfig(DynamoDBMapperConfig.ConsistentReads.CONSISTENT);
		CatalogItem updatedItem = mapper.load(CatalogItem.class, 601, config);
		System.out.println("Retrieved the previously updated item:");
		System.out.println(updatedItem);
		
		mapper.delete(updatedItem);
		
		CatalogItem deletedItem = mapper.load(CatalogItem.class, updatedItem.getId(), config);
		if(deletedItem == null){
			System.out.println("Done - Sample item is delted.");
		}
				
		
		
	}
	
	
	
	
}
