//package cloudcomputing;
package com.example.AWSImageRekognitionPipeline;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

public class ObjectRekognition {


	static String bucketName = "cs643-njit-project1";   
	static String sqsQueueUrl = "https://sqs.us-east-1.amazonaws.com/093063614472/CS643-RekognitionQueue";
	static String sqsQueueName = "CS643-RekognitionQueue1.fifo";

	public static void main(String[] args) throws IOException,InterruptedException, JMSException {

		// Creating a File object that represents the disk file.
		PrintStream outputStream = new PrintStream(new File("/tmp/ec2-1-output.txt"));

		// Assign o to output stream
		System.setOut(outputStream);


		AWSCredentials credentials = null;

		System.out.println("Initializing S3 connection..");
		AmazonS3 s3Client = InitAWSS3(credentials);	

		System.out.println("Initializing SQS connection..");
		//Create new sqs connection
		//SQSConnectionFactory connectionFactory = new SQSConnectionFactory(new ProviderConfiguration(), AmazonSQSClientBuilder.defaultClient());
		SQSConnectionFactory connectionFactory = new SQSConnectionFactory(new ProviderConfiguration(), AmazonSQSClientBuilder.standard());

		// Create the connection
		SQSConnection connection = connectionFactory.createConnection();

		// Get the wrapped client
		AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();

		// Create an Amazon SQS FIFO queue named MyQueue.fifo, if it doesn't already exist
		if (!client.queueExists(sqsQueueName)) {
			Map<String, String> attributes = new HashMap<String, String>();
			attributes.put("FifoQueue", "true");
			attributes.put("ContentBasedDeduplication", "true");
			client.createQueue(new CreateQueueRequest().withQueueName(sqsQueueName).withAttributes(attributes));
		}

		// Create the nontransacted session with AUTO_ACKNOWLEDGE mode
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		// Create a queue identity and specify the queue name to the session
		Queue queue = session.createQueue(sqsQueueName);

		// Create a producer for the 'MyQueue'
		MessageProducer producer = session.createProducer(queue);  

		try {           

			System.out.println("Fetching S3 objects..");
			List<S3ObjectSummary> objectSummary = FetchS3Objects(s3Client);

			for(S3ObjectSummary s3Obj : objectSummary ) {

				String img = s3Obj.getKey();   
				System.out.println("Detecting labels for S3 objects..");
				DetectLabelsandSendMsg(img , session, producer);       		

			}

			TextMessage message = session.createTextMessage("-1");
			message.setStringProperty("JMSXGroupID", "Default");
			producer.send(message);
			System.out.println("JMS Message " + message.getJMSMessageID());
			System.out.println("JMS Message Sequence Number " + message.getStringProperty("JMS_SQS_SequenceNumber")); 

			System.out.println();


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
        	 
           
   	        if(session != null)session.close();
            if(connection != null)connection.close();

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

		//AmazonS3 s3Client=AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
		return s3Client;
	}

	/// List s3 objects  	 
	private static List<S3ObjectSummary> FetchS3Objects(AmazonS3 s3) {

		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
		ListObjectsV2Result objectListing = s3.listObjectsV2(req);	   
		//objectListing.se
		List<S3ObjectSummary> objectSummary = objectListing.getObjectSummaries();
		return objectSummary;		
	}

	// Detects labels of the images fetched from s3 bucket
	private static void DetectLabelsandSendMsg(String img, Session session, MessageProducer producer) throws JMSException {

		AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();

		//AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

		DetectLabelsRequest request = new DetectLabelsRequest()
				.withImage(new Image()
						.withS3Object(new S3Object()
								.withName(img)
								.withBucket(bucketName))).withMaxLabels(10).withMinConfidence(75F);

		try {                    	
			DetectLabelsResult detectLabelsResult = rekognitionClient.detectLabels(request);
			List<Label> labels = detectLabelsResult.getLabels();		 

			Hashtable<String, Integer> numbers = new Hashtable<String , Integer>();
			for (Label label : labels) 
			{				
				if(label.getName().equals("Car") & label.getConfidence()>80) 
				{						
					System.out.print("Detected labels for:  " + img +" => ");
					numbers.put(label.getName(), Math.round(label.getConfidence()));
					System.out.print("Label: " + label.getName() + " ,");
					System.out.print("Confidence: " + label.getConfidence().toString() + "\n");
					System.out.println("Sending msg to SQS..CS643-RekognitionQueue");
					TextMessage message = session.createTextMessage(img);
					message.setStringProperty("JMSXGroupID", "Default");
					producer.send(message);
					System.out.println("JMS Message " + message.getJMSMessageID());
					System.out.println("JMS Message Sequence Number " + message.getStringProperty("JMS_SQS_SequenceNumber")); 
				}                                     
			}  

		} 		
		catch (AmazonRekognitionException e) {
			e.printStackTrace();
		}
	}

}

