package cloudcomputing;
//package com.example.AWSImageRekognitionPipeline;


import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.jms.Queue;
import javax.jms.Session;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.DetectTextRequest;
import com.amazonaws.services.rekognition.model.DetectTextResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.rekognition.model.TextDetection;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.TextMessage;


public class TextRekognition {

	static String bucketName = "cs643-njit-project1";   
	static String sqsQueueUrl = "https://sqs.us-east-1.amazonaws.com/093063614472/CS643-RekognitionQueue";
	static String sqsQueueName = "CS643-RekognitionQueue1.fifo";

	public static void main(String[] args) throws JMSException, InterruptedException , IOException {

    Session session = null;
    MessageConsumer consumer = null;
    SQSConnection connection = null;
		try {

			// Creating a File object that represents the disk file.
			//PrintStream outputStream = new PrintStream(new File("\\Programming Assignment\\ec2-2-output.txt"));
			PrintStream outputStream = new PrintStream(new File("/tmp/ec2-2-output.txt"));

			// Assign o to output stream
			System.setOut(outputStream);

			System.out.println("Initializing SQS connection..");
			// Create a new connection factory with all defaults (credentials and region) set automatically
			SQSConnectionFactory connectionFactory = new SQSConnectionFactory(new ProviderConfiguration(), AmazonSQSClientBuilder.defaultClient());

			// Create the connection
			 connection = connectionFactory.createConnection();

			// Get the wrapped client
			AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();

			if (!client.queueExists(sqsQueueName)) {
				Map<String, String> attributes = new HashMap<String, String>();
				attributes.put("FifoQueue", "true");
				attributes.put("ContentBasedDeduplication", "true");
				client.createQueue(new CreateQueueRequest().withQueueName(sqsQueueName).withAttributes(attributes));
			}

			// Create the nontransacted session with AUTO_ACKNOWLEDGE mode
			 session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			// Create a queue identity and specify the queue name to the session
			Queue queue = session.createQueue(sqsQueueName);

			//Create a consumer for the 'MyQueue'.
			 consumer = session.createConsumer(queue);

			// Instantiate and set the message listener for the consumer.
			consumer.setMessageListener(new Listener(connection,bucketName));

			// Start receiving incoming messages.
			connection.start();

			Thread.sleep(10000);


		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
   finally {
            // To close resources etc
        	 if(consumer!=null) consumer.close();
           
   	        if(session != null)session.close();
            if(connection != null)connection.close();

        }
	}



}

class Listener implements MessageListener{		

	SQSConnection connection = null;
	static String bucketName;

	public Listener(SQSConnection conn, String bucket){		
		this.connection = conn;
		this.bucketName = bucket;
	}

	@Override
	public void onMessage(javax.jms.Message message) {
		try {
			// TODO Auto-generated method stub
			AWSCredentials credentials = null;

			System.out.println("Initializing S3 connection..");

			AmazonS3 s3Client = InitAWSS3(credentials);

			System.out.println("Fetching S3 objects..");
			List<S3ObjectSummary> objectSummary = FetchS3Objects(s3Client);

			for (S3ObjectSummary obj : objectSummary) 
			{
				String msg = (String) ((TextMessage) message).getText().toString();

				if(!msg.equals("-1")) {
					if(obj.getKey().contains(msg)){
						System.out.println("Index of the image " + msg + " found.. Proceeding to detect texts in the image..");
						DetectText(msg);
					}
				}
				else {
        break;
					//System.out.println("Closing the connection..");
					
				}


			}
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	/// Method to initialize S3 client using AWS credentials
	private static AmazonS3 InitAWSS3(AWSCredentials credentials) {
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (C:\\Users\\Nancy\\.aws\\credentials), and is in valid format.",
							e);
		}

		Regions clientRegion = Regions.US_EAST_1;

		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withRegion(clientRegion)
				.build();
		return s3Client;
	}


	/// List s3 objects  	 
	private static List<S3ObjectSummary> FetchS3Objects(AmazonS3 s3) {

		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
		ListObjectsV2Result objectListing = s3.listObjectsV2(req);	       
		List<S3ObjectSummary> objectSummary = objectListing.getObjectSummaries();
		return objectSummary;		
	}

	private static void DetectText(String img) {

		AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();

		DetectTextRequest request = new DetectTextRequest()
				.withImage(new Image()
						.withS3Object(new S3Object()
								.withName(img)
								.withBucket(bucketName)));


		try {
			DetectTextResult result = rekognitionClient.detectText(request);
			List<TextDetection> textDetections = result.getTextDetections();


			if(!textDetections.isEmpty()) {
				
				for (TextDetection text: textDetections) {
					if(text.getConfidence()>80)
					{
           System.out.println("Detected lines and words for " + img);
						System.out.println("Detected: " + text.getDetectedText());
						System.out.println("Confidence: " + text.getConfidence().toString());
						System.out.println("Id : " + text.getId());
						System.out.println("Parent Id: " + text.getParentId());
						System.out.println("Type: " + text.getType());
						System.out.println();
					}
				}				
			}


		} catch(AmazonRekognitionException e) {
			e.printStackTrace();
		}

	}

}
