package example.rawkv;

import java.util.List;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.Key;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

public class Scan {
  public static void main(String[] args) throws Exception {
    scanWithLimit();
    scanAllData();
  }

  private static void scanWithLimit() throws Exception {
    TiConfiguration conf = TiConfiguration.createRawDefault("127.0.0.1:2379");
    TiSession session = TiSession.create(conf);
    RawKVClient client = session.createRawClient();

    // prepare data
    client.put(ByteString.copyFromUtf8("k1"), ByteString.copyFromUtf8("v1"));
    client.put(ByteString.copyFromUtf8("k2"), ByteString.copyFromUtf8("v2"));
    client.put(ByteString.copyFromUtf8("k3"), ByteString.copyFromUtf8("v3"));
    client.put(ByteString.copyFromUtf8("k4"), ByteString.copyFromUtf8("v4"));

    // scan with limit
    int limit = 1000;
    List<Kvrpcpb.KvPair> list = client.scan(ByteString.copyFromUtf8("k1"), ByteString.copyFromUtf8("k5"), limit);
    for (Kvrpcpb.KvPair pair : list) {
      System.out.println(pair);
    }

    // close
    client.close();
    session.close();
  }

  private static void scanAllData() throws Exception {
    TiConfiguration conf = TiConfiguration.createRawDefault("127.0.0.1:2379");
    TiSession session = TiSession.create(conf);
    RawKVClient client = session.createRawClient();

    // prepare data
    String keyPrefix = "prefix";
    for (int i = 1; i <= 9; i++) {
      for (int j = 1; j <= 9; j++) {
        client.put(ByteString.copyFromUtf8(keyPrefix + i + j), ByteString.copyFromUtf8("v" + i + j));
      }
    }

    // scan all data
    ByteString startKey = ByteString.copyFromUtf8(keyPrefix + "11");
    ByteString endKey = Key.toRawKey(ByteString.copyFromUtf8(keyPrefix + "99")).next().toByteString();
    int limit = 4;
    while(true) {
      List<Kvrpcpb.KvPair> list = client.scan(startKey, endKey, limit);
      Key maxKey = Key.MIN;
      for (Kvrpcpb.KvPair pair : list) {
        System.out.println(pair);
        Key currentKey = Key.toRawKey(pair.getKey());
        if(currentKey.compareTo(maxKey) > 0) {
          maxKey = currentKey;
        }
      }

      if(list.size() < limit) {
        break;
      }
      startKey = maxKey.next().toByteString();
    }

    // close
    client.close();
    session.close();
  }
}
