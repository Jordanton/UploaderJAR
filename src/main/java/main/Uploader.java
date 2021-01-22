package main;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.Date;
import java.util.HashMap;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Uploader {
	
	private static final Logger logger = LoggerFactory.getLogger(Uploader.class);

	static final String CONTENT_TYPE = "application/vnd.emc.documentum+json";
	static final String QUERY = "INSERT INTO LATAX.WEB_APP_DOCUMENT_IDS (APP_ID, DOC_ID, DOC_NAME, CREATED_BY) VALUES (?, ?, ?, ?)";

	public static void main(String[] args) throws RestClientException, URISyntaxException {		
		try {
			InputStream input = new FileInputStream("/app/latax/uploader/uploader.properties");
			//InputStream input = new FileInputStream("app.properties");
			Properties prop = new Properties();
			prop.load(input);
			input.close();
			
			String directory = prop.getProperty("directory");
			String apiKey = prop.getProperty("api.key");
			String generalAuthKey = prop.getProperty("general.auth.key");
			String deleteAuthKey = prop.getProperty("delete.auth.key");
			//String folderUrl = prop.getProperty("folder.url");
			String nonProfitUrl = prop.getProperty("nonprofit.url");
			String vendorUrl = prop.getProperty("vendor.url");
			String lifeLineUrl = prop.getProperty("lifeline.url");
			String docUrlBase = prop.getProperty("doc.url");
			
			String dbUrl = prop.getProperty("db.url");
			String dbUser = prop.getProperty("db.username");
			String dbPassword = prop.getProperty("db.password");
			String dbSchema = prop.getProperty("db.schema");		
			
			//Get date
			Calendar calendar = Calendar.getInstance();
			SimpleDateFormat formatter = new SimpleDateFormat("MM-dd-yyyy");	
			formatter.setLenient(false);
		    Date today = calendar.getTime();
		    //Initialize up here for the try-catch block to work
		    RestTemplate rest = new RestTemplate();
			HttpHeaders headers = new HttpHeaders();			
			headers.add("x-api-key", apiKey);
			headers.add("Authorization", generalAuthKey);
			
			MultiValueMap<String, String> deleteHeaders = new LinkedMultiValueMap<String, String>();
		    deleteHeaders.add("x-api-key", apiKey);
		    deleteHeaders.add("Authorization", deleteAuthKey);
		    
		    Connection con = null;		  
			PreparedStatement stmt = null;
			HttpEntity<String> request = null;
			ResponseEntity<String> response = null;
			String objectUrl = null;
			
			// Map to content type
			HashMap<String, String> contentType = new HashMap<String, String>();
			contentType.put("pdf", MediaType.APPLICATION_PDF_VALUE);
			contentType.put("jpg", MediaType.IMAGE_JPEG_VALUE);
			contentType.put("jpeg", MediaType.IMAGE_JPEG_VALUE);
			contentType.put("png", MediaType.IMAGE_PNG_VALUE);
			
			try {
				con=DriverManager.getConnection(dbUrl, dbUser, dbPassword);  		
				stmt=con.prepareStatement(QUERY);  
				File[] rootFolder = new File(directory).listFiles();
				for (File rootFolderItem : rootFolder) {
					try {
						if(!rootFolderItem.isDirectory() 
								|| rootFolderItem.getName().equals(formatter.format(today)) 
								|| formatter.parse(rootFolderItem.getName()).after(today)) {
							continue; //don't upload today's documents
						}
					} catch (ParseException e) {
						e.printStackTrace();
						logger.error(rootFolderItem.getName() + " file/folder encountered.  Please validate.");
						continue;
					}		
					File dateFolder = new File(directory+rootFolderItem.getName());
					File[] files = dateFolder.listFiles();
					for (File file : files) {
						// build request body JSON						 
						try {				    	
							String[] name = file.getName().split("_");
							String folderUrl = "";
							JsonObject values = new JsonObject();
							values.addProperty("object_name", file.getName());
							if(name[0].equals("NP")) {
								values.addProperty("r_object_type", "oof_public_nonprofit_doc");
								folderUrl = nonProfitUrl;
							} else if(name[0].equals("V")) {
								values.addProperty("r_object_type", "oof_public_vendor_doc");
								folderUrl = vendorUrl;
							} else if(name[0].equals("LL")) {
								values.addProperty("r_object_type", "oof_public_lifeline_doc");
								folderUrl = lifeLineUrl;
							}								
							JsonObject properties = new JsonObject();
							properties.add("properties", new JsonParser().parse(values.toString()));
							
							// make first request to generate object and object ID			
							headers.set("Content-Type", CONTENT_TYPE);
							request = new HttpEntity<String>(properties.toString(), headers);			
							response = rest.postForEntity(folderUrl, request, String.class);
							
							//If successful, make second request with bytes data
							if(response.getStatusCode().equals(HttpStatus.CREATED)) {
								JsonObject responseJson = new JsonParser().parse(response.getBody()).getAsJsonObject();			
							    String objectId = responseJson.get("properties").getAsJsonObject().get("r_object_id").getAsString();
							    objectUrl = new StringBuilder(docUrlBase).append(objectId).toString();
							    headers.set( "Content-Type", contentType.get(FilenameUtils.getExtension(file.getName()).toLowerCase()));
							    HttpEntity<byte[]> bytes = new HttpEntity<byte[]>(Files.readAllBytes(file.toPath()), headers);	
							    response = rest.postForEntity(objectUrl+"/contents", bytes, String.class);
							    if(response.getStatusCode().equals(HttpStatus.CREATED)) {
							    	//<app_name>_<app_id>_<filename>.<file_type>	
							    	//"INSERT INTO LATAX.WEB_APP_DOCUMENT_IDS (APP_ID, DOC_ID, DOC_NAME, CREATED_BY) VALUES (?, ?, ?)";
							    	stmt.setString(1, name[1]); 
							    	stmt.setString(2, objectId);
							    	stmt.setString(3, name[2]);
							    	stmt.setString(4, name[0]);
							    	stmt.executeUpdate();
							    }
							}
							logger.info("FILE " + file.getName() + " UPLOAD SUCCESSFUL");
							file.delete(); 						
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							logger.error("FILE " + file.getName() + " UPLOAD FAILED/INCOMPLETE");
							logger.error(e.getMessage());						
						}
					}
					if(dateFolder.list().length == 0) {
						logger.info("DATE " + rootFolderItem.getName() + " UPLOAD SUCCESSFUL");
						dateFolder.delete();
					} else {
						logger.error("DATE " + rootFolderItem.getName() + " UPLOAD FAILED/INCOMPLETE");
					}
				}				
				stmt.close();
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error(e.getMessage());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block				
			e.printStackTrace();
			logger.error(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}
}
