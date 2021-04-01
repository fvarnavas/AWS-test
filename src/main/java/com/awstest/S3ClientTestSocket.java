package com.awstest;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class S3ClientTestSocket {
    private final static Regions region = Regions.US_EAST_1;
    private final static String bucket = "cs-db-test";
    private final static String key = "FrankTest";
    private final static long sendSize = 5000000;
    private static AmazonS3 client;

    public static void main(String[] args) {
        // connect to S3 using cached credentials
        AmazonS3 client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(region)
                .build();

        if (!client.doesBucketExist(bucket))
            client.createBucket(bucket);

        try {
            // bind the server socket to an ephemeral port
            ServerSocket serverSocket = new ServerSocket(0);
            final int port = serverSocket.getLocalPort();
            final String host = serverSocket.getInetAddress().getHostAddress();
            System.out.printf("Started server on %s:%d%n", host, port);

            // start the client thread that will call the server and send the [size:payload]
            Runnable sender = ()->{
                System.out.printf("Starting client connecting to server on %s:%d%n", host, port);

                try (Socket socket = new Socket(host, port);
                    InputStream is = new GeneratedInputStream(sendSize);
                    OutputStream os = socket.getOutputStream()){

                    System.out.printf("Sending a %d byte payload to server%n", sendSize);

                    int c;
                    while ((c = is.read()) != -1){
                        os.write((byte)c);
                    }
                    System.out.println("Done sending, calling close");
                    os.close();

                    System.out.println("Done sending to server");
                } catch( Exception ex){
                    System.err.println("Error sending data to server. " + ex);
                }
            };

            new Thread(sender).start();

            // accept the client connection
            Socket socket = serverSocket.accept();
            System.out.printf("Client connected%n");

            // process the socket input stream
            try(InputStream is = new BufferedInputStream(socket.getInputStream())){

                long contentLength = S3Util.getContentLength(is);
                System.out.printf("putObject started, contentLength=%,d bytes%n", contentLength);

                long lengthReturned = S3Util.putStream(client, bucket, key, contentLength, is);
                System.out.printf("putObject completed. ContentLength=%,d bytes%n", lengthReturned);
            } catch( Exception ex){
                System.err.println("Error sending stream to S3. " + ex);
            }
        } catch ( Exception ex){
            System.err.println("Error sending stream to S3. " + ex);
            ex.printStackTrace();
        }
    }
}
