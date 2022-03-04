Refer to the requirements for this code in Requirements.txt

To configure and run, follow the below steps.

1. Create two EC2 instances in AWS
2. Download keys to SSH into EC2 instance
3. Set up AWS SDK environment for java in both the instances - Run ec2-env-setup/env_setup.bash
4. Configure AWS credentials in the linux machine. Access key, Secret key and token are required to be updated in ~/.aws/credentials file 
5. Create a Maven project to interact with AWS services using java SDK -> Execute the script /ec2-env-setup/mvn_project_setup.bash
6. Update pom.xml with all maven and aws sdk dependencies
7. To build the project -> mvn clean install  -Dmaven.test.skip=true
8. To execute the code -> mvn -Xexec:java -Dexec.mainClass="com.example.AWSImageRekognitionPipeline.ObjectRekognition"
