package org.apache;

import org.apache.minibase.*;

import java.io.IOException;

/**
 * @author chengzw
 * @description
 * @since 2021/7/31
 */
public class Main {
    public static void main(String[] args) throws IOException {
        String dataDir = "/Users/chengzhiwei/tmp/minibase";
        Config conf = new Config().setDataDir(dataDir).setMaxMemstoreSize(100).setFlushMaxRetries(1)
                .setMaxDiskFiles(10);
        MiniBase db = MStore.create(conf).open();
        // Put
        db.put(Bytes.toBytes("chengzw"), Bytes.toBytes("good"));
        db.put(Bytes.toBytes("tom"), Bytes.toBytes("bad"));
        db.put(Bytes.toBytes("jack"), Bytes.toBytes("medium"));
        db.put(Bytes.toBytes("mark"), Bytes.toBytes("medium"));
        // Scan
        MiniBase.Iter<KeyValue> kv = db.scan();
        while (kv.hasNext()) {
            System.out.println(kv.next());
        }
    }
}
