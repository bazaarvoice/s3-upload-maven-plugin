s3-upload-maven-plugin
======================
Uploads a file or (recursively) the contents of a directory to S3.

Configuration parameters
------------------------

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
|bucketName|The name of the bucket|*yes*| |
|source|The source file or folder (was sourceFile before 1.2)|*yes*| |
|destination|The destination file or destination folder (was destinationFile before 1.2) if unspecified uploads to the root of the bucket| *no*| |
|recursive|If this is a directory copy, recursively copy all contents (since 1.2)| *no* | false |
|accessKey|S3 access key | *no* | if unspecified, uses the Default Provider, falling back to env variables |
|secretKey|S3 secret key | *no* | if unspecified, uses the Default Provider, falling back to env variables |
|doNotUpload|Dry run| *no* | false |
|compress|If true, gzip compresses all files and sets the Content-Encoding metadata| *no* | false |
|endpoint|Use a different s3 endpoint| *no* | s3.amazonaws.com |

Example: Upload a file
----------------------
```xml
<build>
  ...

  <plugins>
    ...

    <plugin>
      <groupId>com.bazaarvoice.maven.plugins</groupId>
      <artifactId>s3-upload-maven-plugin</artifactId>
      <version>1.2</version>
      <configuration>
        <bucketName>my-s3-bucket</bucketName>
        <source>dir/filename.txt</source>
        <destination>remote-dir/remote-filename.txt</destination>
      </configuration>
    </plugin>
  </plugins>
</build>
```

Example: Recursively upload a folder
------------------------------------
```xml
<build>
  ...

  <plugins>
    ...

    <plugin>
      <groupId>com.bazaarvoice.maven.plugins</groupId>
      <artifactId>s3-upload-maven-plugin</artifactId>
      <version>1.0</version>
      <configuration>
        <bucketName>my-s3-bucket</bucketName>
        <source>dir</source>
        <destination>remote-dir</destination>
        <recursive>true</recursive>
      </configuration>
    </plugin>
  </plugins>
</build>
```

