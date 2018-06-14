import org.apache.hadoop.hbase.util.Bytes;

public class t {
    public static void main(String[] args) {
        String a = "82736516";
        byte[] b = Bytes.toBytes(a);
        System.out.println(b.length);
    }
}
