package com.awstest;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.InputStream;

public class S3ClientTest {
    public static void main(String[] args) {
        Regions region = Regions.US_EAST_1;
        String bucket = "cs-db-test";
        String key = "FrankTest";

        // connect to S3 using cached credentials
        AmazonS3 client = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(new ProfileCredentialsProvider())
                    .withRegion(region)
                    .build();

        if(!client.doesBucketExist(bucket))
            client.createBucket(bucket);

        // Create an input stream to send 300MB
        try(InputStream is = new GeneratedInputStream(300000000L)) {
            int c;
            byte[] tmp = new byte[10];
            int count = 0;

            // read the size from the input stream
            while((c = is.read()) != -1 && count < 10){
                tmp[count++] = (byte)c;
            }

            // set the content length
            long contentLength = Long.parseLong(new String(tmp));
            System.out.println(String.format("contentLength is %,d bytes", contentLength));
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(contentLength);

            // stream the payload to S3
            try {
                System.out.println("putObject started");
                client.putObject(new PutObjectRequest(bucket, key, is, metadata));
                System.out.println("putObject completed");

                System.out.println(String.format("InstanceLength=%,d bytes", client.getObjectMetadata(bucket, key).getInstanceLength()));
            } catch (AmazonServiceException ase) {
                System.out.println("Error Message:    " + ase.getMessage());
                System.out.println("HTTP Status Code: " + ase.getStatusCode());
                System.out.println("AWS Error Code:   " + ase.getErrorCode());
                System.out.println("Error Type:       " + ase.getErrorType());
                System.out.println("Request ID:       " + ase.getRequestId());
            } catch (AmazonClientException ace) {
                System.out.println("Error Message: " + ace.getMessage());
            }
        } catch( Exception ex){
            System.err.println("Error sending stream to S3" + ex);
        }
    }
}
