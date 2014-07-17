package com.bazaarvoice.maven.plugins.s3.upload;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.ObjectMetadataProvider;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

@Mojo(name = "s3-upload")
public class S3UploadMojo extends AbstractMojo implements ObjectMetadataProvider
{
  /** Access key for S3. */
  @Parameter(property = "s3-upload.accessKey")
  private String accessKey;

  /** Secret key for S3. */
  @Parameter(property = "s3-upload.secretKey")
  private String secretKey;

  /**
   *  Execute all steps up except the upload to the S3.
   *  This can be set to true to perform a "dryRun" execution.
   */
  @Parameter(property = "s3-upload.doNotUpload", defaultValue = "false")
  private boolean doNotUpload;

  /** The file/folder to upload. */
  @Parameter(property = "s3-upload.source", required = true)
  private String source;

  /** The bucket to upload into. */
  @Parameter(property = "s3-upload.bucketName", required = true)
  private String bucketName;

  /** The file/folder (in the bucket) to create.
   * If this is not present or empty, will upload to the root of the bucket */
  @Parameter(property = "s3-upload.destination")
  private String destination;

  /** Force override of endpoint for S3 regions such as EU. */
  @Parameter(property = "s3-upload.endpoint")
  private String endpoint;

  /** In the case of a directory upload, recursively upload the contents. */
  @Parameter(property = "s3-upload.recursive", defaultValue = "false")
  private boolean recursive;

  /** In the case of a directory upload, recursively upload the contents. */
  @Parameter(property = "s3-upload.compress", defaultValue = "false")
  private boolean compress;


    @Override
  public void execute() throws MojoExecutionException
  {
    File sourceFile = new File(source);
    if (!sourceFile.exists()) {
      throw new MojoExecutionException("File/folder doesn't exist: " + source);
    }

    AmazonS3 s3 = getS3Client(accessKey, secretKey);
    if (endpoint != null) {
      s3.setEndpoint(endpoint);
    }

    if (!s3.doesBucketExist(bucketName)) {
      throw new MojoExecutionException("Bucket doesn't exist: " + bucketName);
    }

    if (doNotUpload) {
      getLog().info(String.format("File %s would have be uploaded to s3://%s/%s (dry run)",
        sourceFile, bucketName, destination));

      return;
    }

    if (compress) {
        getLog().info("Compressing files prior to upload");
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File compressed = (sourceFile.isFile())?new File(tempDir, sourceFile.getName()):new File(tempDir, UUID.randomUUID().toString());
        long saved = compress(sourceFile,compressed);
        getLog().info(String.format("Saved a total of %s bytes via compression", saved));
        sourceFile = compressed;
        compressed.deleteOnExit();
    }

    boolean success = upload(s3, sourceFile);
    if (!success) {
      throw new MojoExecutionException("Unable to upload file to S3.");
    }

    getLog().info(String.format("File %s uploaded to s3://%s/%s",
      sourceFile, bucketName, destination));
  }

  private long compress(File from, File to)
          throws MojoExecutionException
  {
      long savings = 0;
      if (from==null || to==null)
      {
          throw new MojoExecutionException("From and To both must exist (from="+from+", to="+to+")");
      }

      if (from.isFile())
      {
          try {
              byte[] buffer = new byte[1024];
              GZIPOutputStream out =
                      new GZIPOutputStream(new FileOutputStream(to));
              FileInputStream in =
                      new FileInputStream(from);
              int len;
              while ((len = in.read(buffer)) > 0) {
                  out.write(buffer, 0, len);
              }

              in.close();

              out.finish();
              out.close();
              savings = to.length()-from.length();
          }
          catch (IOException ioe)
          {
              throw new MojoExecutionException("Error attempting to zip file",ioe);
          }
      }
      else // directory
      {
          to.mkdirs();

          for (String s:from.list())
          {
              savings+=compress(new File(from,s),new File(to,s));
          }
          getLog().debug(String.format("Saved %s bytes on directory %s",savings,from));
          return savings;
      }
      getLog().debug(String.format("Saved %s bytes on %s",savings,from));
      return savings;
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

  private boolean upload(AmazonS3 s3, File sourceFile) throws MojoExecutionException
  {
    TransferManager mgr = new TransferManager(s3);

    Transfer transfer;
    if (sourceFile.isFile()) {
      ObjectMetadata omd = new ObjectMetadata();
      provideObjectMetadata(sourceFile, omd);
        try {
            transfer = mgr.upload(bucketName, destination, new FileInputStream(sourceFile), omd);
        } catch (IOException ioe)
        {
            throw new MojoExecutionException("Couldn't open stream to file", ioe);
        }
    } else if (sourceFile.isDirectory()) {
      transfer = mgr.uploadDirectory(bucketName, destination, sourceFile, recursive, this);
    } else {
      throw new MojoExecutionException("File is neither a regular file nor a directory " + sourceFile);
    }
    try {
      getLog().debug("Transferring " + transfer.getProgress().getTotalBytesToTransfer() + " bytes...");
      transfer.waitForCompletion();
      getLog().info("Transferred " + transfer.getProgress().getBytesTransfered() + " bytes.");
    } catch (InterruptedException e) {
      return false;
    }

    return true;
  }

  @Override
  public void provideObjectMetadata(File file, ObjectMetadata objectMetadata) {
      if (compress) {
          objectMetadata.setContentEncoding("gzip");
      }
  }

}
