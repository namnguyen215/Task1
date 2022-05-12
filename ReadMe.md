
### **2. apcdx**
- **Chạy task bằng spark-submit:**

`  spark-submit --class apcdxTask\
  --deploy-mode client\
  --num-executors 5 \
  --executor-cores 2 \
  --executor-memory 2G \
  target/InternTask-1.0-SNAPSHOT.jar`
- **Kết quả các task được lưu dưới dạng parquet tại:**
  - Đếm số  GUID theo từng bannerid theo ngày: hdfs:/DataIntern2022/result/apcdx/res1
  - Đếm số  GUID theo từng bannerid theo tháng: hdfs:/DataIntern2022/result/apcdx/res2
  - Tính toán việc phân bổ bannerid theo từng domain: hdfs:/DataIntern2022/result/apcdx/res3