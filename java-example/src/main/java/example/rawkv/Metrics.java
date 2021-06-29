package example.rawkv;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

public class Metrics {
  public static void main(String[] args) throws Exception {
    TiConfiguration conf1 = TiConfiguration.createRawDefault("127.0.0.1:2379");
    conf1.setMetricsEnable(true);
    TiSession session = TiSession.create(conf1);
    RawKVClient client = session.createRawClient();

    while (true) {
      // put
      client.put(ByteString.copyFromUtf8("k1"), ByteString.copyFromUtf8("Hello"));
      client.put(ByteString.copyFromUtf8("k2"), ByteString.copyFromUtf8(","));
      client.put(ByteString.copyFromUtf8("k3"), ByteString.copyFromUtf8("World"));
      client.put(ByteString.copyFromUtf8("k4"), ByteString.copyFromUtf8("!"));
      client.put(ByteString.copyFromUtf8("k5"), ByteString.copyFromUtf8("Raw KV"));

      // get
      Optional<ByteString> result = client.get(ByteString.copyFromUtf8("k1"));
      System.out.println(result.get().toStringUtf8());

      // batch get
      List<Kvrpcpb.KvPair> list =client.batchGet(new ArrayList<ByteString>() {{
        add(ByteString.copyFromUtf8("k1"));
        add(ByteString.copyFromUtf8("k3"));
      }});
      System.out.println(list);

      // scan
      list = client.scan(ByteString.copyFromUtf8("k1"), ByteString.copyFromUtf8("k6"), 10);
      System.out.println(list);

      Thread.sleep(10000);
    }
  }
}
