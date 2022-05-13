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
    private SparkSession spark ;
    static String dataPath = "hdfs:/DataIntern2022/data/ppcv/*";
    static String resPath1 = "hdfs:/DataIntern2022/result/ppcv/res1";
    static String resPath2 = "hdfs:/DataIntern2022/result/ppcv/res2";
    static String resPath3 = "hdfs:/DataIntern2022/result/ppcv/res3";
    Dataset<Row> getData() {
        spark = SparkSession.builder().master("yarn").getOrCreate();
        Dataset<Row> df = spark.read().format("parquet").load(dataPath);
        return df;
    }
    private void writeToParquet(Dataset<Row> dataset,String path){
        dataset.write().parquet(path);
    }
    private void topDomainByGuid(Dataset<Row> df){
        //Lấy top 5 domain có số lượng GUID nhiều nhất.
        Dataset<Row> res1 = df
                .select(col("domain").cast("String"),col("guid")) //Lấy ra các 2 cột cần tính toán
                .groupBy(col("domain"))                                     //Nhóm các guid có cùng domain
                .agg(count("guid"))                                        //Đếm tổng số GUID của mỗi domain
                .orderBy(col("count(guid)").desc())                         //Sắp xếp theo thứ tự giảm dần của số GUID
                .limit(5);                                                       //Lấy ra 5 bản ghi có số lượng GUID lớn nhất

        res1=res1.withColumnRenamed("count(guid)","NumberOfGuids");
        writeToParquet(res1,resPath1);
    }
    private void topLocationyGuid(Dataset<Row> df){
        //Lấy top 5 vị trí địa lý có nhiều GUID truy cập nhất. Vị trí địa lý sử dụng trường locid >1.


        Dataset<Row> res2=df.filter("locid>1")              //Lấy ra các cột có locid>1
                .select(col("locid"),col("guid"))       // Lấy ra 2 cột cần tính toán
                .groupBy(col("locid"))                          //Nhóm các guid có cùng locid
                .agg(count("guid"))                          //Đếm tổng số GUID của mỗi locid
                .orderBy(col("count(guid)").desc())             //Sắp xếp theo thứ tự giảm dần của số GUID
                .limit(5);                                          //Lấy ra 5 bản ghi có số lượng GUID lớn nhất

        res2=res2.withColumnRenamed("count(guid)","NumberOfGuids");
        res2.write().parquet(resPath2);
        writeToParquet(res2,resPath2);
    }

    private void GGFBRate(Dataset<Row> df){
        //Tính tỉ lệ pageview phát sinh từ google, fb.

        //@pageViewGoogle: Lưu số bản ghi phát sinh từ Google
        long pageViewGoogle = df.select(col("refer").cast("String"))
                .filter(col("refer")
                        .rlike("google.com|com.google")).count();
        //@pageViewFacebook: Lưu số bản ghi phát sinh từ Facebook
        long pageViewFacebook =  df.select(col("refer").cast("String"))
                .filter(col("refer")
                        .rlike("facebook.com")).count();
        //@total: Lưu tổng số bản ghi
        long total=df.count();
        StructType structType = new StructType();
        structType = structType.add("source", DataTypes.StringType, false);
        structType = structType.add("rate", DataTypes.DoubleType, false);

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create("Google", (pageViewGoogle*1.0/total)*100));
        nums.add(RowFactory.create("Facebook", (pageViewFacebook*1.0/total)*100));
        Dataset<Row> res3 = spark.createDataFrame(nums, structType);    //Tạo dataset lưu dữ liệu vừa tính toán được
        writeToParquet(res3,resPath3);
    }
    private void startSparkJob(){
        Dataset<Row> df = getData();
        topDomainByGuid(df);
        topLocationyGuid(df);
        GGFBRate(df);
    }

    public static void main(String[] args) {
//        SparkConf sparkConf = new SparkConf().setAppName("ppcvTask")
//                .set("spark.dynamicAllocation.enabled", "true")
//                .set("spark.dynamicAllocation.shuffleTracking.enabled","true")
//                .set("spark.shuffle.service.enabled","true")
//                .set("spark.dynamicAllocation.minExecutors", "1")
//                .set("spark.dynamicAllocation.maxExecutors", "20");
        ppcvTask Task=new ppcvTask();
        Task.startSparkJob();


    }
}
