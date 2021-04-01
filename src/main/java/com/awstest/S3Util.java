package com.awstest;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

import java.io.InputStream;

public class S3Util {
    /**
     * Stream a payload to S3
     * @param client
     * @param bucket
     * @param key
     * @param contentLength
     * @param is
     * @return
     */
    public static long putStream(AmazonS3 client, String bucket, String key, long contentLength, InputStream is) {
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(contentLength);

            client.putObject(new PutObjectRequest(bucket, key, is, metadata));
            return client.getObjectMetadata(bucket, key).getContentLength();
        } catch (AmazonServiceException ex) {
            throw new RuntimeException(ExceptionToString(ex),ex);
        } catch (AmazonClientException ex) {
            throw new RuntimeException(ExceptionToString(ex), ex);
        }
    }

    /**
     * Parse the 10 byte length from the start of stream
     * @param is
     * @return length of the stream
     */
    public static long getContentLength(InputStream is) {
        try {
            byte[] tmp = new byte[10];

            if (is.read(tmp) < tmp.length)
                throw new RuntimeException("Input stream too short");

            return Long.parseLong(new String(tmp));
        } catch (Exception ex) {
            System.err.println("Error reading/parsing object length " + ex);
            throw new RuntimeException(ex);
        }
    }

    public static String ExceptionToString(AmazonServiceException ase){
        StringBuilder buffer = new StringBuilder();
        buffer.append("Error Message:    ").append(ase.getMessage()).append("\n");
        buffer.append("HTTP Status Code: ").append(ase.getStatusCode()).append("\n");
        buffer.append("AWS Error Code:   ").append(ase.getErrorCode()).append("\n");
        buffer.append("Error Type:       ").append(ase.getErrorType()).append("\n");
        buffer.append("Request ID:       ").append(ase.getRequestId()).append("\n");
        return buffer.toString();
    }

    public static String ExceptionToString(AmazonClientException ace){
        return "Error Message: " + ace.getMessage();
    }
}
