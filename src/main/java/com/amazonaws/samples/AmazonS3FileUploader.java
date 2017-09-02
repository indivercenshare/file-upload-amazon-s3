package com.amazonaws.samples;
import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class AmazonS3FileUploader {
	static final Logger logger = Logger.getLogger(AmazonS3FileUploader.class);

	private static String bucketName = "acbbindiver";
	static String destinationPath = "";
	private static AmazonS3 s3client = null;
	static String[] arguments = {};
	static boolean isFirstTime = true;
	static List<String> filetypes = new ArrayList<String>();
	static Scanner sc = new Scanner(System.in);


	private static void processDir(String folderName, File folder) {
		if (folder.listFiles() != null) {
			for (final File fileEntry : folder.listFiles()) {
				if (fileEntry.isDirectory()) {
					processDir(folderName, fileEntry);
				}
				if (!fileEntry.isDirectory()) {
					processFile(folderName, fileEntry);
				}

			}
		} else if (!folder.isDirectory()) {
			processFile(folderName, folder);
		}

	}

	private static void processFile(String folderAbName, File fileEntry) {

		String filePath = null;
		if (!fileEntry.getAbsolutePath().equalsIgnoreCase(folderAbName)) {
			filePath = fileEntry.getAbsolutePath().replaceAll(folderAbName, "");

		} else {
			filePath = fileEntry.getName();
		}
		String fileExtension = "*." + FilenameUtils.getExtension(filePath);
		if (filetypes.contains("*.*") || filetypes.contains(fileExtension)) {

			// create a PutObjectRequest passing the folder name suffixed by /
			// send request to S3 to create folder
			// AmazonS3FileUploader.s3client.putObject(new
			// PutObjectRequest(bucketName,filePath, emptyContent, metadata));
			if(fileEntry.exists()){
				filePath =filePath.substring(1, filePath.length());
				logger.info("Uploading File========"+fileEntry.getName() + " At location " + filePath);
				AmazonS3FileUploader.s3client.putObject(new PutObjectRequest(bucketName, filePath, fileEntry)
						.withCannedAcl(CannedAccessControlList.PublicRead));
				moveFile(fileEntry);
			}else{
				if(fileEntry.getName() != null && fileEntry.getName() != null){
					logger.info("Missing File Name============" + fileEntry.getName() + " At location " + fileEntry.getName());
				}
				
			}
			
		}

	}
	
	private static void moveFile(File fileEntry) {
		try{
			String sourcepath = AmazonS3FileUploader.arguments[3];
		    File sourceFolder = new File(sourcepath);
	    	if(sourceFolder.isDirectory()){
	    		
	    		File destinationFolder = new File(AmazonS3FileUploader.destinationPath);
	    		if(!destinationFolder.exists()){
	    			destinationFolder.mkdir();
	    		}
	    		logger.info("Moving File====== "+fileEntry.getName());
	    		FileUtils.copyFileToDirectory(fileEntry, destinationFolder);
	    		boolean movingStatus = fileEntry.delete();
	    		logger.info("Moving File====== "+fileEntry.getName() + " Status=>>> " +  movingStatus);
	    	}	
    	}catch(Exception e){
    		logger.error("Some Problem Occured While Moving " + fileEntry.getName());
    	}
	}

	private static void configureLog4j() {
	    Properties props = new Properties();
	    props.put("log4j.rootLogger", "INFO, file, stdout, errorAppender");

	    String projectPath = System.getProperty("user.dir");
	    System.out.println("Project Location " + projectPath);
	    if(!projectPath.endsWith("/")){
	    	projectPath = projectPath + "/logs" ;
	    }
	    File logFolder = new File(projectPath);
	    if(!logFolder.exists()){
	    	logger.info("Log Folder Created");
	    	logFolder.mkdir();
	    }
	    String logPath = projectPath + "/app.log";
	    System.out.println("App.log " + logPath);
	    logger.info("App Log File Processed");
	    props.put("log4j.appender.file.File",logPath);
	    props.put("log4j.appender.file", "org.apache.log4j.DailyRollingFileAppender");
	    props.put("log4j.appender.file.DatePattern", "'.'yyyy-MM-dd");
	    props.put("log4j.appender.file.layout", "org.apache.log4j.PatternLayout");
	    props.put("log4j.appender.file.layout.ConversionPattern", "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n");

	
	    props.put("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
	    props.put("log4j.appender.stdout.Target", "System.out");
	    props.put("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
	    props.put("log4j.appender.stdout.layout.ConversionPattern", "%d{yyyy-MM-dd HH:mm:ss} %-5p - %m%n");
	   
	    String errorLogPath = projectPath + "/app-error.log";
	    System.out.println("App.log " + errorLogPath);
	    logger.info("App Error Log File Processed");
	    props.put("log4j.appender.errorAppender.File",errorLogPath);
	    props.put("log4j.appender.errorAppender", "org.apache.log4j.DailyRollingFileAppender");
	    props.put("log4j.appender.errorAppender.Threshold", "ERROR");
	    props.put("log4j.appender.errorAppender.DatePattern", "'.'yyyy-MM-dd");
	    props.put("log4j.appender.errorAppender.layout", "org.apache.log4j.PatternLayout");
	    props.put("log4j.appender.errorAppender.layout.ConversionPattern", "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n");
	   
	    PropertyConfigurator.configure(props);
	  }

	public static void main(String[] args) {
		// arg[0] denotes ACCESS KEY
		// arg[1] denotes SECRET ACCESS KEY
		// arg[2] denotes BUCKET NAME
		// arg[3] denotes File path
		// arg[4] denotes Region Name
		// arg[5] denotes Moving File Path
		// AKIAJSVEZJ473PNAVRHA DCxi6sDrkK0srHOLCPPBrek9YE6L2WFZyvA+b18a
		// censhare-indiver40 /Users/ink/Desktop/ZIP
		configureLog4j();
		//PropertyConfigurator.configure("log4j.properties");
		
	
		arguments = args;
		Runnable task = new UploaderTask();
		if (arguments.length > 3) {
			ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
			File input = new File(arguments[3]);
			if (input.isDirectory()) {
				long timeInMiliSecs = inputTime();
				filetypes = inputFileTypes();
				executor.scheduleWithFixedDelay(task,0, timeInMiliSecs, TimeUnit.MILLISECONDS);
			} else {
				task.run();
			}
		}

	}
	
	private static List<String> inputFileTypes() {
			String fileTypeStr = readLine("Please Enter File Types Seperated By Commas like *.rar,*.zip or *.*");
			return Arrays.asList(fileTypeStr.split(","));
	}

	public static long inputTime(){
		long timeInMiliSecs = 0l;
		try {
			logger.info(
					"Please enter the Time unit\n1. Press 1 for Hours\n2. Press 2 for Minutes\n3. Press 3 for Seconds\n4. Press 4 for Miliseconds");
			long time = 0;
			int choice = sc.nextInt();
			switch (choice) {
			case 1:
				logger.info("You have choosen time unit in Hours");
				logger.info("Please Enter No of hours[Please Enter numerical values only]");
				time = sc.nextLong();
				timeInMiliSecs = time * 60 * 60 * 1000;
				break;
			case 2:
				logger.info("You have choosen time unit in Minutes");
				logger.info("Please Enter No of Minutes[Please Enter numerical values only]");
				time = sc.nextLong();
				timeInMiliSecs = time * 60 * 1000;
				break;
			case 3:
				logger.info("You have choosen time unit in Seconds");
				logger.info("Please Enter No of Seconds[Please Enter numerical values only]");
				time = sc.nextLong();
				timeInMiliSecs = time * 1000;
				break;
			case 4:
				logger.info("You have choosen time unit in MiliSeconds");
				logger.info("Please Enter No of MiliSeconds[Please Enter numerical values only]");
				time = sc.nextLong();
				timeInMiliSecs = time;
				break;
			default:
				logger.info("Wrong Choice Entered");
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			System.exit(0);
		}


		destinationPath = readLine("Please Enter Destination Path");
		return timeInMiliSecs;
	}
	
	private static String readLine(String prompt) {
		logger.info(prompt);
        String line = null;
        Console c = System.console();
        if (c != null) {
             line = c.readLine();
        } else {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
            try {
                 line = bufferedReader.readLine();
            } catch (IOException e) { 
                //Ignore    
            }
        }
        return line;
    }

	@SuppressWarnings("deprecation")
	public static void uploadZip(String[] args) {
		boolean isTesting = false;
		String accessKey = "AKIAJSVEZJ473PNAVRHA";
		String secretKey = "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";
		String filePath = "/Users/ink/Desktop/ZIP";
		String region = "ap-south-1";
		logger.info("\n==============================UPLOADING FILES==============================\n");
		if (!isTesting) {
			logger.info("CHECKING ACCESS KEY");
			if (!(args.length > 0) || args[0] == null || args[0] == "") {
				logger.info("\t\t\tNOT FOUND");
				System.exit(0);
			} else {
				accessKey = args[0];
				logger.info("\t\t\t" + accessKey);
			}
			logger.info("CHECKING SECRET ACCESS KEY");
			if (!(args.length > 1) || args[1] == null || args[1] == "") {
				logger.info("\t\t\tNOT FOUND");
				System.exit(0);
			} else {
				secretKey = args[1];
				logger.info("\t\t" + secretKey);
			}
			logger.info("CHECKING BUCKET NAME");
			if (!(args.length > 2) || args[2] == null || args[2] == "") {
				logger.info("\t\t\tNOT FOUND");
				System.exit(0);
			} else {
				bucketName = args[2];
				logger.info("\t\t\t" + bucketName);
			}
			logger.info("CHECKING FILE/FOLDER PATH");
			if (!(args.length > 3) || args[3] == null || args[3] == "") {
				logger.info("\t\t\tNOT FOUND");
				System.exit(0);
			} else {
				filePath = args[3];
				logger.info("\t\t" + filePath);
			}
			logger.info("CHECKING REGION");
			if (args.length < 5) {
				logger.info("\t\t\tRegion ap-south-1 would be used or specify region.");
				logger.info("=======================AVAILABLE REGIONS====================");
				logger.info("1. us-gov-west-1");
				logger.info("2. us-east-1");
				logger.info("3. us-east-2");
				logger.info("4. us-west-1");
				logger.info("5. us-west-2");
				logger.info("6. eu-west-1");
				logger.info("7. eu-west-2");
				logger.info("8. eu-central-1");
				logger.info("9. ap-south-1");
				logger.info("10. ap-south-1");
				logger.info("11. ap-southeast-1");
				logger.info("13. ap-southeast-2");
				logger.info("14. ap-northeast-1");
				logger.info("15. ap-northeast-2");
				logger.info("16. sa-east-1");
				logger.info("17. cn-north-1");
				logger.info("18. ca-central-1");
				logger.info("=============================================================");
			}

			if (args.length > 4) {
				region = args[4];
				logger.info("\t\t\t\t" + region);
			}
		}
		// create a client connection based on credentials
		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		s3client = new AmazonS3Client(credentials);
		
		Region usWest2 = Region.getRegion(Regions.fromName(region));
		s3client.setRegion(usWest2);

		try {
			// create bucket - name must be unique for all S3 users
			boolean isBucketExist = false;
			for (Bucket bucket : s3client.listBuckets()) {
				if (bucket.getName().equalsIgnoreCase(bucketName)) {
//					logger.info("Bucket is already Existed=====>>>> " + bucketName);
					logger.info("Using Existing Bucket" + bucketName);
					isBucketExist = true;
					break;
				}
			}
			if (!isBucketExist) {
				do{
					logger.info("Creating New Bucket===>>" + bucketName);
					if(!s3client.doesBucketExist(bucketName)){
						s3client.createBucket(bucketName);	
						break;
					}else{
						bucketName = readLine("Sorry Bucket " + bucketName + " is already Exist.Please Enter Other Name");
					}	
				}while(true);
				
				
			}
			//
			File folder = new File(filePath);
			processDir(filePath, folder);

		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		
		logger.info("\n==============================UPLOADED==========================================\n");
		

	}

}
