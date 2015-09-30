package dynamoDB.persistence;

import java.awt.print.Book;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshaller;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshalling;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.ec2.model.VolumeDetail;
import com.fasterxml.jackson.annotation.JsonFormat.Value;

import dynamoDB.scan.Initial;

public class ObjectPersistenceMappingExample {

	private static AmazonDynamoDBClient client;
	
	public static void main(String[] args) {
		Initial.init();
		
		client = Initial.getClient();
		
		
		DimensionType dimType = new DimensionType();
		dimType.setHeight("8.0");
		dimType.setLength("11.0");
		dimType.setThickness("1.0");
		
		
		Book book = new Book();
		book.setId(502);
		book.setTitle("Book 502");
		book.setISBN("555-5555555555");
		book.setBookAuthors(new HashSet<String>(Arrays.asList("Author1", "Author2")));
		book.setDimensions(dimType);
		
		System.out.println(book);
		
		DynamoDBMapper mapper = new DynamoDBMapper(client);
		
		mapper.delete(book);
		mapper.save(book);
		
		Book bookRetrieved = mapper.load(Book.class, 502);
		
		System.out.println(bookRetrieved);
		
		bookRetrieved.getDimensions().setHeight("2.0");
		bookRetrieved.getDimensions().setLength("12.0");
		bookRetrieved.getDimensions().setThickness("2.0");
		
		mapper.save(bookRetrieved);
		
		bookRetrieved = mapper.load(Book.class, 502);
		System.out.println(bookRetrieved);
		
	}
	
	
	
	@DynamoDBTable(tableName="ProductCatalog")
	public static class Book{
		private int id;
		private String title;
		private String ISBN;
		private Set<String> bookAuthors;
		private DimensionType dimensionType;
		
		
		@DynamoDBHashKey(attributeName="Id")
		public int getId(){return id;}
		public void setId(int id) {this.id = id;}
		
		@DynamoDBAttribute(attributeName="Title")
		public String getTitle(){return title;}
		public void setTitle(String title){this.title = title;}
		
		@DynamoDBAttribute(attributeName="ISBN")
		public String getISBN(){return ISBN;}
		public void setISBN(String ISBN){this.ISBN = ISBN;}
		
		@DynamoDBAttribute(attributeName="Authors")
		public Set<String> getBookAuthors() {return bookAuthors;}
		public void setBookAuthors(Set<String> bookAuthors) {this.bookAuthors = bookAuthors;}
		
		@DynamoDBMarshalling(marshallerClass= DimensionTypeConverter.class)
		public DimensionType getDimensions() {return dimensionType;}
		public void setDimensions(DimensionType dimensionType) {this.dimensionType = dimensionType;}
		
		
		@Override
		public String toString(){
			return "Book [ISBN="+ ISBN + ", bookAuthors="+bookAuthors
					+ ", dimensionType=" + dimensionType + ", Id=" + id
					+ ", Title=" + title +"]";
		}
		
	}
	
	static public class DimensionType{
		private String length;
		private String height;
		private String thickness;
		
		public String getLength() {return length;}
		public void setLength(String length) {this.length=length;}
		
		
		public String getHeight() {return height;}
		public void setHeight(String height){this.height = height;}
		
		
		public String getThickness() {return thickness;}
		public void setThickness(String thickness){this.thickness = thickness;}
		
		
		
		
	}
	
	static public class DimensionTypeConverter implements  DynamoDBMarshaller<DimensionType> {

		@Override
		public String marshall(DimensionType value) {

			DimensionType itemDimensions = (DimensionType)value;
			String dimension = null;
			try {
				if(itemDimensions != null){
					dimension = String.format("%s x $s x %s", 
							itemDimensions.getLength(),
							itemDimensions.getHeight(),
							itemDimensions.getThickness());
					
					
				}
			} catch (Exception e) {

				e.printStackTrace();
			
			}
			
			
			return dimension;
		}

		@Override
		public DimensionType unmarshall(Class<DimensionType> dimensionType, String value) {

			DimensionType itemDimension = new DimensionType();
			try {
				
				if(value != null && value.length() != 0){
					
					String [] data = value.split("x");
					itemDimension.setLength(data[0].trim());
					itemDimension.setHeight(data[1].trim());
					itemDimension.setThickness(data[2].trim());
					
					
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			
			}
			
			
			return itemDimension;
		}
		
		
		
		
		
	}
	
	
	
	
	
}