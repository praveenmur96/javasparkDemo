import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import org.apache.spark.graphx.*;
import java.util.ArrayList;
import java.util.List;

public class SparkGraphx {
    SparkGraphx() {
        SparkSession spark = SparkSession.builder()
                .appName("GraphX Java Example")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<Tuple2<Object, String>> vertexList = new ArrayList<>();
        vertexList.add(new Tuple2<>(1L, "Alice"));
        vertexList.add(new Tuple2<>(2L, "Bob"));
        vertexList.add(new Tuple2<>(3L, "Charlie"));
        vertexList.add(new Tuple2<>(4L, "David"));

        JavaRDD<Tuple2<Object, String>> verticesRDD = sc.parallelize(vertexList);

        List<Edge<String>> edgeList = new ArrayList<>();
        edgeList.add(new Edge<>(1L, 2L, "friends"));
        edgeList.add(new Edge<>(2L, 3L, "colleagues"));
        edgeList.add(new Edge<>(3L, 4L, "family"));
        edgeList.add(new Edge<>(4L, 1L, "neighbors"));

        JavaRDD<Edge<String>> edgesRDD = sc.parallelize(edgeList);

        System.out.println("Vertices:");

        sc.stop();
    }
}