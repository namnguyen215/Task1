import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class ppcvTask {
    public static void main(String[] args) {
//        SparkConf sparkConf = new SparkConf().setAppName("ppcvTask")
//                .set("spark.dynamicAllocation.enabled", "true")
//                .set("spark.dynamicAllocation.shuffleTracking.enabled","true")
//                .set("spark.shuffle.service.enabled","true")
//                .set("spark.dynamicAllocation.minExecutors", "1")
//                .set("spark.dynamicAllocation.maxExecutors", "20");
        SparkSession spark = SparkSession.builder().master("yarn").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = spark.read().format("parquet").load("hdfs:/DataIntern2022/data/ppcv/*");
        //Lấy top 5 domain có số lượng GUID nhiều nhất.
        Dataset<Row> res1 = df
                .select(col("domain").cast("String"),col("guid"))
                .groupBy(col("domain"))
                .agg(count("guid"))
                .orderBy(col("count(guid)").desc())
                .limit(5);
        res1=res1.withColumnRenamed("count(guid)","Numbers of guid");
        res1.show(false);
        //Lấy top 5 vị trí địa lý có nhiều GUID truy cập nhất. Vị trí địa lý sử dụng trường locid >1.
        Dataset<Row> res2=df.filter("locid>1")
                .select(col("locid"),col("guid"))
                .groupBy(col("locid"))
                .agg(count("guid"))
                .orderBy(col("count(guid)").desc())
                .limit(5);
        res2=res2.withColumnRenamed("count(guid)","Numbers of guid");
        res2.show(false);
        //Tính tỉ lệ pageview phát sinh từ google, fb.
        Dataset<Row> pageViewGGandFB=df.select(col("refer").cast("String"))
                .filter(col("refer")
                        .rlike("google.com|com.google|facebook.com"));
        long pageViewGoogle = pageViewGGandFB
                .filter(col("refer")
                        .rlike("google.com|com.google")).count();
        long pageViewFacebook =  pageViewGGandFB
                .filter(col("refer")
                        .rlike("facebook.com")).count();
        long total=df.count();
        StructType structType = new StructType();
        structType = structType.add("source", DataTypes.StringType, false);
        structType = structType.add("rate", DataTypes.DoubleType, false);

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create("Google", (pageViewGoogle*1.0/total)*100));
        nums.add(RowFactory.create("Facebook", (pageViewFacebook*1.0/total)*100));
        Dataset<Row> res3 = spark.createDataFrame(nums, structType);
        res3.show(false);
    }
}
