package com.awstest;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.BufferedInputStream;
import java.io.InputStream;

/**
 * Test sending a stream to S3
 * Uses GeneratedInputStream, no sockets
 * I tested with JVM set to 7MB
 */

public class S3ClientTest {
    public static void main(String[] args) {
        Regions region = Regions.US_EAST_1;
        String bucket = "cs-db-test";
        String key = "FrankTest";
        long sendSize = 300000000;

        // connect to S3 using cached credentials
        AmazonS3 client = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(new ProfileCredentialsProvider())
                    .withRegion(region)
                    .build();

        if(!client.doesBucketExist(bucket))
            client.createBucket(bucket);

        // Create an input stream, send it to S3
        try(InputStream is = new BufferedInputStream(new GeneratedInputStream(sendSize))) {
            long contentLength = S3Util.getContentLength(is);
            System.out.printf("putObject started, contentLength=%,d bytes%n", contentLength);

            long lengthReturned = S3Util.putStream(client, bucket, key, contentLength, is);
            System.out.printf("putObject completed. ContentLength=%,d bytes%n", lengthReturned);
        } catch( Exception ex){
            System.err.println("Error sending stream to S3. " + ex);
        }
    }
}
