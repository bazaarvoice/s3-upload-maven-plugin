package com.bazaarvoice.maven.plugins.s3.upload;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;

@Mojo(name = "s3-upload")
public class S3UploadMojo extends AbstractMojo
{
  /** Access key for S3. */
  @Parameter(property = "s3-upload.accessKey")
  private String accessKey;

  /** Secret key for S3. */
  @Parameter(property = "s3-upload.secretKey")
  private String secretKey;

  /** The file to upload. */
  @Parameter(property = "s3-upload.sourceFile")
  private String sourceFile;

  /** The folder to upload. */
  @Parameter(property = "s3-upload.sourceFolder")
  private String sourceFolder;

  /** The bucket to upload into. */
  @Parameter(property = "s3-upload.bucketName", required = true)
  private String bucketName;

  /** The file (in the bucket) to create. */
  @Parameter(property = "s3-upload.destinationFile", required = true)
  private String destinationFile;

  /** Force override of endpoint for S3 regions such as EU. */
  @Parameter(property = "s3-upload.endpoint")
  private String endpoint;

  @Override
  public void execute() throws MojoExecutionException
  {
    File source = getSource(); // may be a file or a directory

    AmazonS3 s3 = getS3Client(accessKey, secretKey);
    if (endpoint != null) {
      s3.setEndpoint(endpoint);
    }

    if (!s3.doesBucketExist(bucketName)) {
      throw new MojoExecutionException("Bucket doesn't exist: " + bucketName);
    }

    boolean success = upload(s3, bucketName, destinationFile, source);
    if (!success) {
      throw new MojoExecutionException("Unable to upload file to S3.");
    }

    getLog().info("File " + source + " uploaded to s3://" + bucketName + "/" + destinationFile);
  }

  private File getSource() throws MojoExecutionException
  {
    if (sourceFile != null && sourceFolder != null) {
      throw new MojoExecutionException("Specify either sourceFile or sourceFolder (not both).");
    }

    if (sourceFile != null) {
      File foundFile = new File(sourceFile);
      if (!foundFile.exists() || !foundFile.isFile()) {
        throw new MojoExecutionException("File doesn't exist or is not a regular file: " + sourceFile);
      }
      return foundFile;

    } else if (sourceFolder != null) {
      File foundFolder = new File(sourceFolder);
      if (!foundFolder.exists() || !foundFolder.isDirectory()) {
        throw new MojoExecutionException("Folder doesn't exist or is not a directory: " + sourceFolder);
      }
      return foundFolder;

    } else {
      throw new MojoExecutionException("Must specify either sourceFile or sourceFolder.");
    }
  }

  private static AmazonS3 getS3Client(String accessKey, String secretKey)
  {
    AWSCredentialsProvider provider;
    if (accessKey != null && secretKey != null) {
      AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
      provider = new StaticCredentialsProvider(credentials);
    } else {
      provider = new DefaultAWSCredentialsProviderChain();
    }

    return new AmazonS3Client(provider);
  }

  private static boolean upload(AmazonS3 s3, String bucketName, String destination, File source) throws MojoExecutionException
  {
    if (source.isFile()) {
      return uploadFile(s3, bucketName, destination, source);
    } else if (source.isDirectory()) {
      return uploadDirectory(s3, bucketName, destination, source);
    } else {
      throw new MojoExecutionException("Path represents neither a file or a directory..."); // unlikely according to javadoc
    }
  }

  private static boolean uploadFile(AmazonS3 s3, String bucketName, String destination, File source)
  {
    TransferManager mgr = new TransferManager(s3);
    Upload upload = mgr.upload(bucketName, destination, source);

    return waitForTransfer(upload);
  }

  private static boolean uploadDirectory(AmazonS3 s3, String bucketName, String destination, File source)
  {
    TransferManager mgr = new TransferManager(s3);
    MultipleFileUpload multipleFileUpload = mgr.uploadDirectory(bucketName, destination, source, true);

    return waitForTransfer(multipleFileUpload);
  }

  private static boolean waitForTransfer(Transfer transfer)
  {
    try {
      transfer.waitForCompletion();
    } catch (InterruptedException e) {
      return false;
    }

    return true;
  }
}
