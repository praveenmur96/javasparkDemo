import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;
import java.util.List;

public class SparkRDDExample {
    public SparkRDDExample() {
        SparkSession spark = SparkSession.builder()
                .appName("Spark RDD Java Example")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        JavaRDD<Integer> squaredRDD = rdd.map(x -> x * x);
        JavaRDD<Integer> evenRDD = squaredRDD.filter(x -> x % 2 == 0);

        List<Integer> collectedData = evenRDD.collect();
        long count = evenRDD.count();

        System.out.println("Squared Even Numbers: " + collectedData);
        System.out.println("Count of Even Squared Numbers: " + count);

        sc.stop();
    }
}