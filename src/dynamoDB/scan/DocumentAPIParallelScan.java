package dynamoDB.scan;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;

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
			
			
		} catch (AmazonServiceException ase) {

			System.err.println(ase.getMessage());
		
		}
		
		
		
	}
	
	private static void deleteTable(String tableName, int itemLimit, int numberofThreads){
		
		
		
	}
	
	
}
