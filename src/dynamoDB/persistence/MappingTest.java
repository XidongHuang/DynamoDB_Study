package dynamoDB.persistence;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;

import dynamoDB.scan.Initial;

public class MappingTest {

	private static DynamoDB dynamoDB;
	private static AmazonDynamoDBClient client;
	
	public static void main(String[] args) {
		
		Initial.init();
		
//		dynamoDB = Initial.getDynamoDB();
		client = Initial.getClient();
		
		
		
		
		
		DynamoDBMapper mapper = new DynamoDBMapper(client);
		
		CatalogItem item = new CatalogItem();
		item.setId(102);
		item.setTitle("Book 102 Title");
		item.setISBN("222-2222222222");
		item.setBookAuthors(new HashSet<String>(Arrays.asList("Author 1", "Author 2")));
		item.setSomeProp("Test");
		
		mapper.delete(item);
		
		mapper.save(item);
		
		CatalogItem hashKeyValues = new CatalogItem();
		
		hashKeyValues.setId(102);
		DynamoDBQueryExpression<CatalogItem> queryExpression =
				new DynamoDBQueryExpression<CatalogItem>()
				.withHashKeyValues(hashKeyValues);
		
		List<CatalogItem> itemList = mapper.query(CatalogItem.class,
				queryExpression);
		
		for(int i = 0; i < itemList.size(); i++){
			System.out.println(itemList.get(i).getTitle());
			System.out.println(itemList.get(i).getBookAuthors());
		}
		
		
	}
	
	
}
