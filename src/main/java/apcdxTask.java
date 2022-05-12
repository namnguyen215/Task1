import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;
import java.io.IOException;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class apcdxTask {
    static final SparkSession spark = SparkSession.builder().master("yarn").getOrCreate();
    static String dataPath = "hdfs:/DataIntern2022/data/apcdx/*";
    static String sqlCountGuidByBannerIdByDay = "SELECT time_day,bannerId, count(guid) as numberOfGuids FROM data GROUP BY time_day, bannerId";
    static String res1 = "hdfs:/DataIntern2022/result/apcdx/res1";
    static String sqlCountGuidByBannerIdByMonth = "SELECT time_month,bannerId, sum(numberOfGuids) as numberOfGuids FROM data1 GROUP BY time_month, bannerId";
    static String res2 = "hdfs:/DataIntern2022/result/apcdx/res2";
    static String sqlCountBannerIdByDomain = "SELECT domain, count(bannerId) as numberOfBannerIds FROM data GROUP BY domain";
    static String res3 = "hdfs:/DataIntern2022/result/apcdx/res3";

    static Dataset<Row> getData() {
        Dataset<Row> df = spark.read().format("parquet").load(dataPath);
        df = df.filter("click_or_view == 'false'")
                .select((col("time_group.time_create").divide(1000).cast("timestamp").as("time_create"))
                        , col("bannerId"), col("domain").cast("String"), col("guid"))
                .withColumn("time_day", substring(col("time_create"), 1, 10))
                .drop(col("time_create"));
        return df;
    }
    private void writeToParquet(Dataset<Row> dataset,String path){
        dataset.write().parquet(path);
    }

    private Dataset<Row> countGuidByBannerIdByDay(Dataset<Row> df, String query, String path){
        //Đếm số  GUID theo từng bannerid theo ngày
        df.createOrReplaceTempView("data");
        Dataset<Row> dataset = spark.sql(query);
        writeToParquet(dataset,path);
        return dataset;
    }

    private void countGuidByBannerIdByMonth(Dataset<Row> df, String query,String path){
        //Đếm số  GUID theo từng bannerid theo tháng
        df = df
                .withColumn("time_month", substring(col("time_day"), 1, 7))
                .drop(col("time_day"));
        df.createOrReplaceTempView("data1");
        Dataset<Row> sqlDF2 = spark.sql(query);
        writeToParquet(sqlDF2,path);
    }
    private void countBannerIdByDomain(String query, String path){
        //Tính toán việc phân bổ bannerid theo từng domain
        Dataset<Row> sqlDF3 = spark.sql(query);
        writeToParquet(sqlDF3,path);
    }
    void startSparkJob(){
        Dataset<Row> df = getData();
        Dataset<Row> sqlDF1=countGuidByBannerIdByDay(df,sqlCountGuidByBannerIdByDay,res1);
        countGuidByBannerIdByMonth(sqlDF1,sqlCountGuidByBannerIdByMonth,res2);
        countBannerIdByDomain(sqlCountBannerIdByDomain,res3);
    }

    public static void main(String[] args) throws IOException {
        apcdxTask Task=new apcdxTask();
        Task.startSparkJob();
    }
}
