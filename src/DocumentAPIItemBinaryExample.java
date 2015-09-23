// Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under the Apache License, Version 2.0.

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.beans.factory.annotation.InitDestroyAnnotationBeanPostProcessor;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;

public class DocumentAPIItemBinaryExample {
    
    static DynamoDB dynamoDB;
    
    static AmazonDynamoDBClient client;
            
    static String tableName = "Reply";
    static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    
    
    private static void init() throws Exception{
    	
    	AWSCredentials credentials =  null;
    	
    	try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
    		
		} catch (Exception e) {
			throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/home/tony/.aws/credentials), and is in valid format.",
                    e);
		}
    	
    	client = new AmazonDynamoDBClient(credentials);
    	Region usWEST2 = Region.getRegion(Regions.US_WEST_2);
    	client.setRegion(usWEST2);
    	dynamoDB = new DynamoDB(client);
    	
    	
    	
    }
    
    
    
    
    public static void main(String[] args) throws Exception {
        init();
    	
    	try {
    
            // Format the primary key values
            String threadId = "Amazon DynamoDB#DynamoDB Thread 2";
            
            dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            String replyDateTime = dateFormatter.format(new Date());
                
            // Add a new reply with a binary attribute type
            createItem(threadId, replyDateTime);
            
            // Retrieve the reply with a binary attribute type
            retrieveItem(threadId, replyDateTime);
            
            // clean up by deleting the item
            deleteItem(threadId, replyDateTime);
        } catch (Exception e) {
            System.err.println("Error running the binary attribute type example: " + e);
            e.printStackTrace(System.err);
        }
    }

    
    public static void createItem(String threadId, String replyDateTime) throws IOException {
        
        Table table = dynamoDB.getTable(tableName);

        // Craft a long message
        String messageInput = "Long message to be compressed in a lengthy forum reply";
        
        // Compress the long message
        ByteBuffer compressedMessage = compressString(messageInput.toString());
        
        table.putItem(new Item()
            .withPrimaryKey("Id", threadId)
            .withString("ReplyDateTime", replyDateTime)
            .withString("Message", "Long message follows")
            .withBinary("ExtendedMessage", compressedMessage)
            .withString("PostedBy", "User A"));
    }
        
    public static void retrieveItem(String threadId, String replyDateTime) throws IOException {
        
        Table table = dynamoDB.getTable(tableName);
        
        GetItemSpec spec = new GetItemSpec()
            .withPrimaryKey("Id", threadId, "ReplyDateTime", replyDateTime)
            .withConsistentRead(true);

        Item item = table.getItem(spec);
        
        
     // Uncompress the reply message and print
        String uncompressed = uncompressString(ByteBuffer.wrap(item.getBinary("ExtendedMessage")));
        
        System.out.println("Reply message:\n"
            + " Id: " + item.getString("Id") + "\n" 
            + " ReplyDateTime: " + item.getString("ReplyDateTime") + "\n" 
            + " PostedBy: " + item.getString("PostedBy") + "\n"
            + " Message: " + item.getString("Message") + "\n"
            + " ExtendedMessage (uncompressed): " + uncompressed + "\n");
    }
      
    public static void deleteItem(String threadId, String replyDateTime) {
        
        Table table = dynamoDB.getTable(tableName);
        table.deleteItem("Id", threadId, "ReplyDateTime", replyDateTime);
    }
    
    private static ByteBuffer compressString(String input) throws IOException {
        // Compress the UTF-8 encoded String into a byte[]
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream os = new GZIPOutputStream(baos);
        os.write(input.getBytes("UTF-8"));
        os.finish();
        byte[] compressedBytes = baos.toByteArray();
        
        // The following code writes the compressed bytes to a ByteBuffer.
        // A simpler way to do this is by simply calling ByteBuffer.wrap(compressedBytes);  
        // However, the longer form below shows the importance of resetting the position of the buffer 
        // back to the beginning of the buffer if you are writing bytes directly to it, since the SDK 
        // will consider only the bytes after the current position when sending data to DynamoDB.  
        // Using the "wrap" method automatically resets the position to zero.
        ByteBuffer buffer = ByteBuffer.allocate(compressedBytes.length);
        buffer.put(compressedBytes, 0, compressedBytes.length);
        buffer.position(0); // Important: reset the position of the ByteBuffer to the beginning
        return buffer;
    }
    
    private static String uncompressString(ByteBuffer input) throws IOException {
        byte[] bytes = input.array();
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPInputStream is = new GZIPInputStream(bais);
        
        int chunkSize = 1024;
        byte[] buffer = new byte[chunkSize];
        int length = 0;
        while ((length = is.read(buffer, 0, chunkSize)) != -1) {
            baos.write(buffer, 0, length);
        }
        
        return new String(baos.toByteArray(), "UTF-8");
    }
} 