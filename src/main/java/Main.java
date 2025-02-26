import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.*;
import java.io.Serializable;
import java.util.Properties;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;

public class Main implements Serializable{

    public static void main(String[] args) throws Exception, StreamingQueryException {

        dataFrameDemo();
        machineLearningDemo();

        PropertyConfigurator.configure("src/main/resources/log4j.properties");
    }

    public static void dataFrameDemo(){
        SparkSession spark = SparkSession.builder()
                .appName("MySQLSparkApp")
                .master("local[*]")
                .getOrCreate();


        String url = "jdbc:mysql://localhost:3306/mydb";
        String user = "root";
        String password = "Pravkfin123";
        String query = "SELECT code FROM abc";

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", password);
        connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver");

        Dataset<Row> customerDF = spark.read().jdbc(url, "customer", connectionProperties);
        System.out.println("Showing Customer Table: ");
        customerDF.show(10);

        System.out.println("Selecting only id and name");
        Dataset<Row> selectedData = customerDF.select("id","name");
        selectedData.show();

        System.out.println("Counting customers by Designation");
        Dataset<Row> designationCount = customerDF
                .groupBy("designation")
                .count()
                .orderBy(functions.desc("count"));
        designationCount.show();

        System.out.println("Filtering Software Engineers:");
        Dataset<Row> softwareEngineers = customerDF.filter("designation = 'Software Engineers'");
        softwareEngineers.show();

        spark.stop();
    }

    public static void machineLearningDemo(){
        PropertyConfigurator.configure("src/main/resources/log4j.properties");
        SparkSession spark = SparkSession.builder()
                .appName("MySQLSparkApp")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        String url = "jdbc:mysql://localhost:3306/mydb";
        String user = "root";
        String password = "Pravkfin123";

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", password);
        connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver");

        Dataset<Row> customerDF = spark.read().jdbc(url, "customer", connectionProperties);
        System.out.println("Showing Customer Table: ");
        customerDF.show(5);

        StringIndexer indexer = new StringIndexer()
                .setInputCol("designation")
                .setOutputCol("designationIndex");
        Dataset<Row> indexedData = indexer.fit(customerDF).transform(customerDF);


        VectorAssembler assembler=new VectorAssembler()
                .setInputCols(new String[]{"designationIndex"})
                .setOutputCol("features");
        Dataset<Row> trainingData = assembler.transform(indexedData)
                .select("features", "salary"); // Assuming salary column exists

        System.out.println("\n Training Data:");
        trainingData.show(5, false);

        LinearRegression lr = new LinearRegression()
                .setLabelCol("salary")
                .setFeaturesCol("features");
        LinearRegressionModel model= lr.fit(trainingData);

        System.out.println("\n Model Trained Successfully!");

        Dataset<Row> predictions = model.transform(trainingData);
        System.out.println("\n Predictions:");
        predictions.select("features", "salary", "prediction").show(5, false);

        spark.stop();
    }
}
