### **1. ppcv**
- Đề bài:
  - Lấy top 5 domain có số lượng GUID nhiều nhất.
  - Lấy top 5 vị trí địa lý có nhiều GUID truy cập nhất. Vị trí địa lý sử dụng trường locid >1.
  - Tính tỉ lệ pageview phát sinh từ google, fb. Sử dụng trường refer để giải quyết
- Cách làm:
  - Lấy ra các cột cần thiết từ dataset ban đầu, gom nhóm và tổng hợp.
  - Ghi kết quả ra file
- Chạy task bằng lệnh spark-submit:

`  spark-submit --class ppcvTask\
  --deploy-mode client\
  --num-executors 5 \
  --executor-cores 2 \
  --executor-memory 2G \
  target/Task1-1.0-SNAPSHOT.jar`




- **Kết quả các task được lưu dưới dạng parquet tại:**
  - Lấy top 5 domain có số lượng GUID nhiều nhất.: hdfs:/DataIntern2022/result/ppcv/res1
    - ![img.png](ResultDemo/img.png)
  - Lấy top 5 vị trí địa lý có nhiều GUID truy cập nhất: hdfs:/DataIntern2022/result/ppcv/res2
    - ![img_1.png](ResultDemo/img_1.png)
  - Tính tỉ lệ pageview phát sinh từ google, fb: hdfs:/DataIntern2022/result/ppcv/res3
    - ![img_2.png](ResultDemo/img_2.png)

### **2. apcdx**
- Đề bài:
  - Đếm số  GUID theo từng bannerid theo ngày
  - Đếm số  GUID theo từng bannerid theo tháng
  - Tính toán việc phân bổ bannerid theo từng domain
- Cách làm:
  - Sử dụng câu lệnh SQL để giải quyết bài toán
  - Ghi kết quả ra file
- **Chạy task bằng spark-submit:**

`  spark-submit --class apcdxTask\
  --deploy-mode client\
  --num-executors 5 \
  --executor-cores 2 \
  --executor-memory 2G \
  target/Task1-1.0-SNAPSHOT.jar`
- **Kết quả các task được lưu dưới dạng parquet tại:**
  - Đếm số  GUID theo từng bannerid theo ngày: hdfs:/DataIntern2022/result/apcdx/res1
  ![img_3.png](ResultDemo/img_3.png)
  - Đếm số  GUID theo từng bannerid theo tháng: hdfs:/DataIntern2022/result/apcdx/res2
  ![img_4.png](ResultDemo/img_4.png)
  - Tính toán việc phân bổ bannerid theo từng domain: hdfs:/DataIntern2022/result/apcdx/res3
  ![img_5.png](ResultDemo/img_5.png)