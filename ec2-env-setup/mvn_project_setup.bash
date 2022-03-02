echo "Creating directory AWSImageRekognitionPipeline..."

mkdir AWSImageRekognitionPipeline

echo "Changing working directory.."
cd AWSImageRekognitionPipeline

echo "Creating maven project..."
mvn archetype:generate  -DarchetypeGroupId=org.apache.maven.archetypes -DgroupId=com.example.AWSImageRekognitionPipeline -DartifactId=com.example.AWSImageRekognitionPipeline -DinteractiveMode=false


