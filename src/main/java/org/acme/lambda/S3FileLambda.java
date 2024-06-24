package org.acme.lambda;

import static java.lang.String.format;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class S3FileLambda implements RequestHandler<S3Event, Boolean> {

    private final String username = "admin";
    private final String password = "QAZqaz244552";
    private final String url = "jdbc:mysql://database-1.cxomou8k8s1k.us-east-1.rds.amazonaws.com:3306/userDB";
    private final String sqlQueryPattern = "insert into files (file_name, file_size, bucket_name) values ('%s', %d, '%s')";

    @Override
    public Boolean handleRequest(S3Event input, Context context) {
        if (input.getRecords().isEmpty()) {
            return false;
        }

        for (S3EventNotification.S3EventNotificationRecord record : input.getRecords()) {
            String bucketName = record.getS3().getBucket().getName();
            String fileName = record.getS3().getObject().getKey();
            Long fileSize = record.getS3().getObject().getSizeAsLong();

            saveFileData(context, fileName, fileSize, bucketName);
        }
        return true;
    }

    private void saveFileData(Context context, String fileName, Long fileSize, String bucketName) {
        LambdaLogger logger = context.getLogger();
        logger.log("Invoked JDBCSample.saveFileData");

        try {
            Connection conn = DriverManager.getConnection(url, username, password);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(format(sqlQueryPattern, fileName, fileSize, bucketName));
            logger.log("Successfully executed JDBCSample.saveFileData");
        } catch (Exception e) {
            e.printStackTrace();
            logger.log("Caught exception on invocation JDBCSample.saveFileData: " + e.getMessage());
        }
    }
}
