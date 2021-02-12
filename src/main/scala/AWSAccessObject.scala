import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.S3Object
import java.io.File
import com.amazonaws.services.s3.model.ObjectListing

class  MyAWSAccessObject(val key: String, val secretKey: String, val region: String){
    // Set the Key Value from constructor parameter
    final val AWSKeyValue = key
    // Set the Secret Key Value from constructor parameter
    final val AWSSecretValue = secretKey
    // Build the client object using the parameter specified key/secret pair and region
    final val MyS3AWSClient = BuildMyS3Client

    /**
     * A function to build and set the client object
     * @return The AmazonS3 client object
     */
    def BuildMyS3Client: AmazonS3 = {
        val credentials = new BasicAWSCredentials(AWSKeyValue, AWSSecretValue)
        val client = AmazonS3ClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(region)
            .build();
        return client
    }

    /**
      * A function to upload a file to Amazon S3. The parameters determine the bucket path
      * and the file itself.
      * @param bucketFilePath a file path within S3 where we want to place the file
      * @param fileToUpload a java.io.File object containing the file to upload
      */
    def UploadAFile(bucketFilePath: String, fileToUpload: File): Unit = {
        val fileName = fileToUpload.getName()
        // Upload the File as an Amazon S3 Object by client putObject request
        MyS3AWSClient.putObject(bucketFilePath,fileName, fileToUpload)
    }

    /**
      * A function to download a file from the specified S3 Bucket
      * @param bucketFilePath a file path to the bucket we want to download the file from
      * @param fileToDownload a string/key for identifying the file on S3
      * @return The downloaded file as an Amazon S3Object
      * TODO: Convert S3Object to a java.io.File type and/or write the contents to a file locally
      */
    def DownloadAFile(bucketFilePath: String, fileToDownload: String): S3Object = {
        // Grab the File as an Amazon S3 Object by client getObject request
        val theObject = MyS3AWSClient.getObject(bucketFilePath, fileToDownload)
        return theObject
    }

    /**
      * A function to list the files inside a given S3 bucket by key
      * @param bucketFilePath a path to the bucket whose contents we want to list
      * @return the results of listing the objects in an Amazon S3 bucket
      * TODO: Possibly convert ObjectListing to another format, depends on where this is getting called
      * and how they want the information formatted
      */
    def ListS3FilesByBucket(bucketFilePath: String): ObjectListing = {
        // Grab the listing object using client request
        val theObjectListing = MyS3AWSClient.listObjects(bucketFilePath)
        // Test printing to console
        theObjectListing.getObjectSummaries().forEach(x => println(x + "\n"))
        return theObjectListing
    }
}