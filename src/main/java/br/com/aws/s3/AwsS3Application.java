package br.com.aws.s3;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.core.waiters.Waiter;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;

@SpringBootApplication
public class AwsS3Application {

    public static void main(String[] args) {
        SpringApplication.run(AwsS3Application.class, args);
    }


    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {
            String bucketName = "marcelo-test-bucket3"; // tem que estar criado o bucket na aws


            S3Client s3Client = createS3Client();

            createBucket(bucketName, s3Client);

            //listBucketObjects(s3, bucketName);
            s3Client.close();
        };
    }

    private static void createBucket(String bucketName, S3Client s3Client) {
        S3Waiter waiter = s3Client.waiter();
        CreateBucketRequest createBucketRequest = CreateBucketRequest
                .builder()
                .bucket(bucketName)
                .build();
        s3Client.createBucket(createBucketRequest);
        HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();
        WaiterResponse<HeadBucketResponse> waiterResponse = waiter.waitUntilBucketExists(bucketRequestWait);
        waiterResponse.matched().response().ifPresent(System.out::println);
        System.out.println("bucket created");
    }

    private static S3Client createS3Client() {
        Region region = Region.SA_EAST_1;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        return s3;
    }

    public static void listBucketObjects(S3Client s3, String bucketName) {

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
        return val / 1024;
    }

}
