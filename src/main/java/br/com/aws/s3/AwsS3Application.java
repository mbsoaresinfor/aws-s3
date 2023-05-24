package br.com.aws.s3;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

@SpringBootApplication
public class AwsS3Application {

	public static void main(String[] args) {
		SpringApplication.run(AwsS3Application.class, args);
	}


	@Bean
	public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
		return args -> {
			String bucketName = "marcelo-test-bucket"; // tem que estar criado o bucket na aws


			Region region = Region.SA_EAST_1;
			S3Client s3 = S3Client.builder()
					.region(region)
					.build();

			listBucketObjects(s3, bucketName);
			s3.close();
		};
	}

	public static void listBucketObjects(S3Client s3, String bucketName ) {

		try {
			ListObjectsRequest listObjects = ListObjectsRequest
					.builder()
					.bucket(bucketName)
					.build();

			ListObjectsResponse res = s3.listObjects(listObjects);
			List<S3Object> objects = res.contents();
			for (S3Object myValue : objects) {
				System.out.print("\n The name of the key is " + myValue.key());
				System.out.print("\n The object is " + calKb(myValue.size()) + " KBs");
				System.out.print("\n The owner is " + myValue.owner());
			}

		} catch (S3Exception e) {
			System.err.println(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
	}

	private static long calKb(Long val) {
		return val/1024;
	}

}
