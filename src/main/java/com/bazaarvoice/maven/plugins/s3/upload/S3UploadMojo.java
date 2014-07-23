package com.bazaarvoice.maven.plugins.s3.upload;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.ObjectMetadataProvider;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
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

  /** If true, gzip compresses all the files and sets the Content-Encoding metadata to 'gzip'. */
  @Parameter(property = "s3-upload.compress", defaultValue = "false")
  private boolean compress;

    /** If compression enabled, this is a list of regular expressions which, if matched, do not get compressed */
    @Parameter(property = "s3-upload.compressExcludes")
    private List<String> compressExcludes;

    /** If set, this will add a Cache-Control header to all the uploaded objects */
    @Parameter(property = "s3-upload.cacheControl")
    private String cacheControl;


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
          throw new MojoExecutionException("From and To both must be non-null (from="+from+", to="+to+")");
      }

      // We have to copy the file since we will be reading from a different directory
      if (from.isFile())
      {
          try {
              byte[] buffer = new byte[1024];
              OutputStream out = null;
              if (fileExcludedFromCompression(from))
              {
                  out = new FileOutputStream(to);
              }
              else
              {
                  out = new GZIPOutputStream(new FileOutputStream(to));
              }
              FileInputStream in =
                      new FileInputStream(from);
              int len;
              while ((len = in.read(buffer)) > 0) {
                  out.write(buffer, 0, len);
              }

              in.close();
              /*
              if (GZIPOutputStream.class.isAssignableFrom(out.getClass()))
              {
                  ((GZIPOutputStream)out).finish();
              }*/

              /*
              if (isExcluded) {
                  ((out.finish();
              }
              */
              out.close();
              savings = from.length() - to.length();
          } catch (IOException ioe) {
              throw new MojoExecutionException("Error attempting to zip file", ioe);
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
      transfer = mgr.uploadFileList(bucketName, destination, sourceFile.getParentFile(), Arrays.asList(sourceFile), this);
    } else if (sourceFile.isDirectory()) {
            transfer = mgr.uploadDirectory(bucketName, destination, sourceFile, recursive, this);

    } else {
      throw new MojoExecutionException("File is neither a regular file nor a directory " + sourceFile);
    }
    try {
      getLog().info(String.format("About to transfer %s bytes...",  transfer.getProgress().getTotalBytesToTransfer()));
      transfer.waitForCompletion();
        getLog().info(String.format("Completed transferring %s bytes...",  transfer.getProgress().getBytesTransferred()));
    } catch (InterruptedException e) {
      return false;
    }
      catch (AmazonS3Exception as3e)
    {
        getLog().info(String.format("Error from S3 : %s"),as3e);
        return false;
    }

    return true;
  }

  @Override
  public void provideObjectMetadata(File file, ObjectMetadata objectMetadata) {
      getLog().debug(String.format("Creating metadata for %s (size=%s)",file, file.length()));
      if (compress && !fileExcludedFromCompression(file)) {
          objectMetadata.setContentEncoding("gzip");
      }
      if (cacheControl!=null)
      {
          objectMetadata.setCacheControl(cacheControl);
      }
  }

    /**
     * Checks if file matches any of the exclusion patterns.
     * NOTE: Yes, I'm well aware it does a bunch of recalculation of the pattern objects
     * and linear searches multiple times (and doesn't shortcut the loop), but it is simple
     * and this runs per-build.  Not really trying to save the 14 nanoseconds involved.
     * @param f File to check
     * @return boolean true if the file should not be compressed
     */
    public boolean fileExcludedFromCompression(File f)
    {
        boolean rval = false;
        if (f!=null && compressExcludes!=null)
        {
            for (String s:compressExcludes)
            {
                Pattern pattern = Pattern.compile(s);
                if (pattern.matcher(f.getAbsolutePath()).matches())
                {
                    getLog().debug(String.format("File %s matches pattern %s and will be excluded from compression", f, s));
                    rval = true;
                }
            }
        }
        return rval;
    }


}
