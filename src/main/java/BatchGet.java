import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BatchGet implements Runnable {
  private static final Log log = LogFactory.getLog(BatchGet.class);

  String _zookeeper = null;
  String _port = null;
  String _parent = null;
  String _tablename = null;
  String _family = null;
  String _quality = null;
  byte[] _FAMILY = null;
  byte[] _QUALITY = null;
  int _batch = 0;

  public BatchGet(String _zookeeper, String _port, String _parent, String _tablename, String _family, String _quality, int _batch) {
    this._zookeeper = _zookeeper;
    this._port = _port;
    this._parent = _parent;
    this._tablename = _tablename;
    this._family = _family;
    this._quality = _quality;

    if (null != _family) {
      _FAMILY = Bytes.toBytes(_family);
    } else {
      log.error("family is null !");
    }
    if (null != _quality) {
      _QUALITY = Bytes.toBytes(_quality);
    } else {
      log.error("quality is null !");
    }

    this._batch = _batch;
  }

  @Override
  public void run() {
    batch2Hbase();
  }

  private void batch2Hbase() {
    HTable ht = null;
    try {
      ht = new HTable(HbaseConnect.connection(_zookeeper, _parent, _port), _tablename);
      ht.setWriteBufferSize(8 * 1024 * 1024);
      ht.setAutoFlush(false, false);
    } catch (IOException e) {
      e.printStackTrace();
    }

    List<Row> batch = new ArrayList<Row>();

    String str = null;
    int count = 0;
    while (true) {
      try {
        str = HbaseTest.strlist.poll(3, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if (null == str) {
        log.warn("get null line data");
        break;
      }

      byte[] rowkey = Bytes.toBytes(str);
      Get get = new Get(rowkey);
      get.addColumn(_FAMILY, _QUALITY);
      batch.add(get);

      int size = batch.size();
      if (size > 0 && (size % _batch == 0)) {
        long begin = System.currentTimeMillis();
        try {
          ht.batch(batch);
          count++;
        } catch (Throwable t) {
          log.error("batchGetFail", t);
        } finally {
          batch.clear();
          long end = System.currentTimeMillis();
          long cust = end - begin;
          log.info("batch: " + size + ", time: " + cust);
        }
      }
    }
    log.info("ThreadRunLoop: " + count);
    if (ht != null) {
      try {
        ht.close();
        System.gc();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }


}