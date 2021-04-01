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
        long sendSize = 1000;

        // connect to S3 using cached credentials
        AmazonS3 client = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(new ProfileCredentialsProvider())
                    .withRegion(region)
                    .build();

        if(!client.doesBucketExist(bucket))
            client.createBucket(bucket);

        // Create an input stream to send 300MB
        try(InputStream is = new GeneratedInputStream(sendSize)) {
            long contentLength = S3Util.getContentLength(is);
            System.out.printf("putObject started, contentLength=%,d bytes%n", contentLength);

            long lengthReturned = S3Util.putStream(client, bucket, key, contentLength, is);
            System.out.printf("putObject completed. ContentLength=%,d bytes%n", lengthReturned);
        } catch( Exception ex){
            System.err.println("Error sending stream to S3. " + ex);
        }
    }
}
