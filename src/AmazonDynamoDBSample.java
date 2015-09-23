/*
 * Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;

/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class AmazonDynamoDBSample {

    /*
     * Before running the code:
     *      Fill in your AWS access credentials in the provided credentials
     *      file template, and be sure to move the file to the default location
     *      (/home/tony/.aws/credentials) where the sample code will load the
     *      credentials from.
     *      https://console.aws.amazon.com/iam/home?#security_credential
     *
     * WARNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

    static AmazonDynamoDBClient dynamoDB;
    static DynamoDB dy;
    /**
     * The only information needed to create a client are security credentials
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints, are performed
     * automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.ProfilesConfigFile
     * @see com.amazonaws.ClientConfiguration
     */
    private static void init() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (/home/tony/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/home/tony/.aws/credentials), and is in valid format.",
                    e);
        }
        dynamoDB = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDB.setRegion(usWest2);
        dy= new DynamoDB(dynamoDB);
    }

    public static void main(String[] args) throws Exception {
        init();

        try {
            String tableName = "Reply";

            // Create table if it does not exist yet
            if (Tables.doesTableExist(dynamoDB, tableName)) {
                System.out.println("Table " + tableName + " is already ACTIVE");
            } else {
                // Create a table with a primary hash key named 'name', which holds a string
                CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                    .withKeySchema(new KeySchemaElement().withAttributeName("name").withKeyType(KeyType.HASH))
                    .withAttributeDefinitions(new AttributeDefinition().withAttributeName("name").withAttributeType(ScalarAttributeType.S))
                    .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
                
                    TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
                    
                System.out.println("Created Table: " + createdTableDescription);

                // Wait for it to become active
                System.out.println("Waiting for " + tableName + " to become ACTIVE...");
                Tables.awaitTableToBecomeActive(dynamoDB, tableName);
            }

            // Describe our new table
            DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
            TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
            System.out.println("Table Description: " + tableDescription);

//            // Add an item
//            Map<String, AttributeValue> item = newItem("Bill & Ted's Excellent Adventure", 1989, "****", "James", "Sara");
//            PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
//            PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
//            System.out.println("Result: " + putItemResult);
//
//            // Add another item
//            item = newItem("Airplane", 1980, "*****", "James", "Billy Bob");
//            putItemRequest = new PutItemRequest(tableName, item);
//            putItemResult = dynamoDB.putItem(putItemRequest);
//            System.out.println("Result: " + putItemResult);

            // Scan items for movies with a year attribute greater than 1985
            HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
            Condition condition = new Condition()
                .withComparisonOperator(ComparisonOperator.GT.toString())
                .withAttributeValueList(new AttributeValue().withN("510"));
            scanFilter.put("PageCount", condition);
            ScanRequest scanRequest = new ScanRequest(tableName);
            ScanResult scanResult = dynamoDB.scan(scanRequest);
            System.out.println("Result: " + scanResult);

            System.out.println("---------");
            listTables();
            System.out.println("----------");
            
            getTableInformation(tableName);
            System.out.println("----------");
            
            //updateTables(tableName);
            
            getTableInformation(tableName);
            System.out.println("--------");
           // deleteTables(tableName);
            
            queryItems(tableName);
            System.out.println("--------");
            
            Table table = dy.getTable("Reply");

            long twoWeeksAgoMilli = (new Date()).getTime() - (300L*24L*60L*60L*1000L);
            Date twoWeeksAgo = new Date();
            twoWeeksAgo.setTime(twoWeeksAgoMilli);
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            String twoWeeksAgoStr = df.format(twoWeeksAgo);

            QuerySpec spec = new QuerySpec()
                .withKeyConditionExpression("Id = :v_id and ReplyDateTime > :v_reply_dt_tm")
                .withFilterExpression("PostedBy = :v_posted_by")
                .withValueMap(new ValueMap()
                    .withString(":v_id", "Amazon DynamoDB#DynamoDB Thread 1")
                    .withString(":v_reply_dt_tm", twoWeeksAgoStr)
                    .withString(":v_posted_by", "User B"))
                .withConsistentRead(true);

            ItemCollection<QueryOutcome> items = table.query(spec);

            Iterator<Item> iterator = items.iterator();
            
            while (iterator.hasNext()) {
            	System.out.println("----Hi----");
                System.out.println(iterator.next());
            }
            
            
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to AWS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with AWS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

	private static void queryItems(String tableName) {
		Table table = dy.getTable(tableName);
		
		Map<String, Object> valueMap = new HashMap<>();
		valueMap.put(":c_id", "Amazon DynamoDB#DynamoDB Thread 1");
		System.out.println(valueMap);
		QuerySpec spec = new QuerySpec()
				.withKeyConditionExpression("Id = :c_id")
				.withValueMap(new ValueMap()
						.withString(":c_id", "Amazon DynamoDB#DynamoDB Thread 1"));
		
		ItemCollection<QueryOutcome> items = table.query(spec);
		
		Iterator<Item> iterator = items.iterator();
		Item item = null;
		while(iterator.hasNext()){
			item = iterator.next();
			System.out.println(item.toJSONPretty());
		}
	}

	private static void deleteTables(String tableName) {
		Table table = dy.getTable(tableName);
		try {
			System.out.println("Issuing DeleteTable request for "+ tableName);
			table.delete();
			
			System.out.println("Waiting for " + tableName
					+ " to be deleted... this may take a while...");
			table.waitForDelete();
			
			if(!Tables.doesTableExist(dynamoDB, tableName)){
				System.out.println("Table " +tableName + " is deleted successfully!");
			} else {
				throw new Exception();
			}
			
			
		} catch (Exception e) {
			System.err.println("DeleteTable request failed for " + tableName);
			System.err.println(e.getMessage());
		
		}
	}

	private static void listTables() {
		TableCollection<ListTablesResult> tables = dy.listTables();
		Iterator<Table> iterator = tables.iterator();
		
		while(iterator.hasNext()){
			Table table = iterator.next();
			System.out.println(table.getTableName());
		}
	}

	private static void updateTables(String tableName) {
		Table table = dy.getTable(tableName);
		System.out.println("MOdifying provisioned throughput for " + tableName);
		
		try{
			table.updateTable(new ProvisionedThroughput()
					.withReadCapacityUnits(10L).withWriteCapacityUnits(5L));
			table.waitForActive();
			
		} catch(Exception e){
			System.err.println("UpdateTable request tailed for "  + tableName);
			System.err.println(e.getMessage());
			
		}
	}

	private static void getTableInformation(String tableName) {
		System.out.println("Describing "+tableName);
		TableDescription ts = dy.getTable(tableName).describe();
		System.out.format("Name: %s \n" + "Status: %s \n" +
							"Provisioned Throughput (read capacity units/sec): %d \n" +
							"Provisioned Throughput (write capacity units/sec): %d \n",
							ts.getTableName(),
							ts.getTableStatus(),
							ts.getProvisionedThroughput().getReadCapacityUnits(),
							ts.getProvisionedThroughput().getWriteCapacityUnits());
	}

    private static Map<String, AttributeValue> newItem(String name, int year, String rating, String... fans) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("name", new AttributeValue(name));
        item.put("year", new AttributeValue().withN(Integer.toString(year)));
        item.put("rating", new AttributeValue(rating));
        item.put("fans", new AttributeValue().withSS(fans));

        return item;
    }

}