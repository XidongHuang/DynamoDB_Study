package dynamoDB.scan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

public class DocumentAPIParallelScan {

	static int scanItemCount = 300;
	
	static int scanItemLimit =10;
	
	static int parallelScanThreads = 16;
	
	static String parallelScanTestTableName = "ParallelScanTest";
	
	static DynamoDB dynamoDB;
	
	public static void main(String[] args) {
		Initial.init();
		
		dynamoDB = Initial.getDynamoDB();
		
		try {
			
			deleteTable(parallelScanTestTableName);
			
			createTable(parallelScanTestTableName, 10L, 5L, "Id", "N");
			
			//Upload sample data for scan
			uploadSampleProducts(parallelScanTestTableName, scanItemCount);
			
			//Scan the table using multiple threads
			parallelScan(parallelScanTestTableName, scanItemCount, parallelScanThreads);
			
			
		} catch (AmazonServiceException ase) {

			System.err.println(ase.getMessage());
		
		}
		
		
		
	}
	
	private static void deleteTable(String tableName){
		
		try {
			
			Table table = dynamoDB.getTable(tableName);
			table.delete();
			System.out.println("Waiting for " + tableName
					+ " to be deleted... this may tabke a while...");
			table.waitForDelete();
			
		} catch (Exception e) {

			System.err.println("Failed to delted table " + tableName);
			e.printStackTrace(System.err);
		
		}
		
	}
	
	private static void createTable(String tableName, long readCapacityUnits,
			long writeCapacityUnits, String hashKeyName, String hashKeyType){
		
		createTable(tableName, readCapacityUnits, writeCapacityUnits, hashKeyName, hashKeyType, null, null);
		
	}
	
	private static void createTable(String tableName, long readCapacityUnits,
			long writeCapacityUnits, String hashKeyName, String hashKeyType,
			String rangeKeyName, String rangeKeyType){
		
		try {
			
			System.out.println("Creating table "+ tableName);
			
			List<KeySchemaElement> keySchema = new ArrayList<>();
			keySchema.add(new KeySchemaElement()
					.withAttributeName(hashKeyName)
					.withKeyType(KeyType.HASH));
			
			List<AttributeDefinition> attributeDefinitions = 
					new ArrayList<>();
			attributeDefinitions.add(new AttributeDefinition()
					.withAttributeName(hashKeyName)
					.withAttributeType(hashKeyType));
			
			if(rangeKeyName != null){
				keySchema.add(new KeySchemaElement()
						.withAttributeName(rangeKeyName)
						.withKeyType(KeyType.RANGE));
				attributeDefinitions.add(new AttributeDefinition()
						.withAttributeName(rangeKeyName)
						.withAttributeType(rangeKeyType));
				
			}
			
			Table table = dynamoDB.createTable(tableName,
					keySchema,
					attributeDefinitions,
					new ProvisionedThroughput()
					.withReadCapacityUnits(readCapacityUnits)
					.withWriteCapacityUnits(writeCapacityUnits));
			System.out.println("Waiting for " + tableName 
					+ " to be created... this may take a while...");
			table.waitForActive();
			
		} catch (Exception e) {

			System.err.println("Failed to create table "+ tableName);
			e.printStackTrace(System.err);
		
		}
		
		
	}
	
	private static void uploadSampleProducts(String tableName, int itemCount){
		
		System.out.println("Adding " + itemCount+" sample items to "
				+ tableName);
		for(int productIndex = 0; productIndex < itemCount; productIndex++){
			
			uploadProducts(tableName, productIndex);
		}
	}
	
	
	
	private static void uploadProducts(String tableName, int productIndex){
		
		Table table = dynamoDB.getTable(tableName);
		
		try {
			
			System.out.println("Processing record #" + productIndex);
			
			Item item = new Item()
					.withPrimaryKey("Id", productIndex)
					.withString("Title", "Book " + productIndex +" Title")
					.withString("ISBN", "111-1111111111")
					.withStringSet("Authors", 
							new HashSet<String>(Arrays.asList("Author1")))
					.withNumber("Price", 2)
					.withString("Dimensions", "8.5 x 11.0 x 0.5")
					.withNumber("PageCount", 500)
					.withBoolean("InPublication", true)
					.withString("ProductCategory", "Book");
			table.putItem(item);
			
		} catch (Exception e) {
			System.err.println("Failed to create item " + productIndex+" in "+ tableName);
			System.err.println(e.getMessage());
			
		}
		
		
		
	}
	
	private static void parallelScan(String tableName, int itemLimit, int numberOfThreads){
		
		System.out.println("Scanning " + tableName + " using " + numberOfThreads
				+ " threads " + itemLimit +" items at a time");
		ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
		
		
		int totalSegments = numberOfThreads;
		for(int segment = 0; segment < totalSegments; segment++){
			
			ScanSegmentTask task = new ScanSegmentTask(tableName, itemLimit, totalSegments, segment);
			
			
			executor.execute(task);
		}
		
		shutDownExecuteorService(executor);
	}
	
	
	private static class ScanSegmentTask implements Runnable{
		
		private String tableName;
		private int itemLimit;
		private int totalSegments;
		
		private int segment;
		
		public ScanSegmentTask(String tableName, int itemLimit, int totalSegments, int segment){
			
			this.tableName = tableName;
			this.itemLimit = itemLimit;
			this.totalSegments = totalSegments;
			this.segment = segment;
			
		}
		
		
		@Override
		public void run(){
			System.out.println("Scanning " + tableName+" segment " + segment
					+ " out of " + totalSegments + " segments " + itemLimit + " items at a time...");
			int totalScannedItemCount = 0;
			
			Table table = dynamoDB.getTable(tableName);
			
			try {
				ScanSpec spec = new ScanSpec()
						.withMaxResultSize(itemLimit)
						.withTotalSegments(totalSegments)
						.withSegment(segment);
				
				ItemCollection<ScanOutcome> items = table.scan(spec);
				Iterator<Item> iterator = items.iterator();
				
				Item currentItem = null;
				
				while(iterator.hasNext()){
					totalScannedItemCount++;
					currentItem = iterator.next();
					System.out.println(currentItem.toJSONPretty());
					
				}
				
			} catch (Exception e) {
				System.err.println(e.getMessage());
			
			} finally {
				
				System.out.println("Scanned " + totalScannedItemCount
						+ " items from segment " + segment +" out of "
						+ totalSegments + " of " + tableName);
				
			}
			
			
			
		}
		
		
	}
	
	
	private static void shutDownExecuteorService(ExecutorService executor){
		executor.shutdown();
		try{
			if(!executor.awaitTermination(10, TimeUnit.SECONDS)){
				executor.shutdownNow();
			}
		} catch(InterruptedException e){
			executor.shutdownNow();
			
			Thread.currentThread().interrupt();
		}
		
	}
	
	
}
