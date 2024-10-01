import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KCombiner extends Reducer<LongWritable, PointWritable, LongWritable, PointWritable> {

    public void reduce(LongWritable centroidId, Iterable<PointWritable> points, Context context)
            throws IOException, InterruptedException {

        // Lấy iterator duy nhất từ points
        java.util.Iterator<PointWritable> iterator = points.iterator();

        // Khởi tạo giá trị đầu tiên
        PointWritable ptSum = PointWritable.copy(iterator.next());
        int pointCount = 1;

        // In ra giá trị của điểm đầu tiên
        System.out.println("Centroid " + centroidId + " has point: " + ptSum);

        // Lặp qua các phần tử còn lại
        while (iterator.hasNext()) {
            PointWritable point = iterator.next();
            ptSum.sum(point);
            System.out.println("Centroid " + centroidId + " has point: " + point);
            pointCount++;
        }

        // Ghi kết quả cuối cùng vào context
        context.write(centroidId, ptSum);

        // In ra số lượng điểm
        System.out.println("Centroid " + centroidId + " has " + pointCount + " points.");
    }
}
