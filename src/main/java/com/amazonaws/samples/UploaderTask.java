package com.amazonaws.samples;
import java.io.File;

import org.apache.log4j.Logger;



public class UploaderTask implements Runnable
{
	static final Logger logger = Logger.getLogger(UploaderTask.class);
		private boolean isActionPerformed = true;
    @Override
    public void run() 
    {
    	
        try {
        	
        	if(isActionPerformed){
        		isActionPerformed=false;
        		String sourcepath = AmazonS3FileUploader.arguments[3];
        	    File sourceFolder = new File(sourcepath);
            	logger.info("*****************************UPLOADING OPERATION SCHEDULAR STARTING**************************");
            	if(sourceFolder.listFiles() == null || sourceFolder.listFiles().length <= 0){
          	    	logger.info("NO FILE AVAILABLE TO UPLOAD\n");
          	    }else{
          	    	AmazonS3FileUploader.uploadZip(AmazonS3FileUploader.arguments);
          	    }
           
            	isActionPerformed=true;
            	logger.info("**************************************UPLOADING OPERATION SCHEDULAR ENDING*****************************************\n");
        	}
        	
        } 
        catch (Exception e) {
        	logger.error(e.getMessage());
        }
    	
        	
    }

	
}