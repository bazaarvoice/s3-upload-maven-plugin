s3-upload-maven-plugin
======================
Uploads a file to S3 from maven.

Example
=======

Upload a file
-------------
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
        <sourceFile>dir/filename.txt</sourceFile>
        <destinationFile>remote-dir/remote-filename.txt</destinationFile>
      </configuration>
    </plugin>
  </plugins>
</build>
```

Upload a directory (recursively)
--------------------------------
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
        <sourceFolder>dir</sourceFile>
        <destinationFile>remote-dir</destinationFile>
      </configuration>
    </plugin>
  </plugins>
</build>
```