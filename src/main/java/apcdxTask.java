import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
public class apcdxTask {
    public static void main(String[] args) throws IOException {
        //Tao session
        SparkSession spark = SparkSession.builder().master("yarn").getOrCreate();
        Dataset<Row> df = spark.read().format("parquet").load("hdfs:/DataIntern2022/data/apcdx/*");
//        Dataset<Row> df = spark.read().format("parquet").load("src/main/resources/apcdx");
        df=df.filter("click_or_view == 'false'")
                .select((col("time_group.time_create").divide(1000).cast("timestamp").as("time_create"))
                        ,col("bannerId"),col("domain").cast("String"),col("guid"))
                .withColumn("time_day",substring(col("time_create"), 1,10))
                .withColumn("time_month",substring(col("time_create"), 1,7))
                .drop(col("time_create"));
        //- Đếm số  GUID theo từng bannerid theo ngày
        System.out.println("So GUID theo bannerId theo ngay");
        df.createOrReplaceTempView("data");
        Dataset<Row> sqlDF1 = spark.sql("SELECT time_day,bannerId, count(guid) as numberOfGuids FROM data GROUP BY time_day, bannerId");
        System.out.println("So GUID theo bannerId theo ngay");
//        sqlDF1.show();
        sqlDF1.write().parquet("hdfs:/DataIntern2022/result/apcdx/res1");
        //- Đếm số  GUID theo từng bannerid theo tháng

        Dataset<Row> sqlDF2 = spark.sql("SELECT time_month,bannerId, count(guid) as numberOfGuids FROM data GROUP BY time_month, bannerId");
        System.out.println("So GUID theo banner theo thang");
//        sqlDF2.show();
        sqlDF2.write().parquet("hdfs:/DataIntern2022/result/apcdx/res2");
        //- Tính toán việc phân bổ bannerid theo từng domain

        Dataset<Row> sqlDF3 = spark.sql("SELECT domain, count(bannerId) as numberOfBannerIds FROM data GROUP BY domain");
        System.out.println("So bannerId theo domain");
//        sqlDF3.show();
        sqlDF3.write().parquet("hdfs:/DataIntern2022/result/apcdx/res3");
    }
}
