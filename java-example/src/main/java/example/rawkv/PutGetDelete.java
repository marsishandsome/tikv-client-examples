package example.rawkv;

import java.util.Optional;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;

public class PutGetDelete {
  public static void main(String[] args) throws Exception {
    TiConfiguration conf = TiConfiguration.createRawDefault("127.0.0.1:2379");
    TiSession session = TiSession.create(conf);
    RawKVClient client = session.createRawClient();

    ByteString key = ByteString.copyFromUtf8("Hello");
    ByteString value = ByteString.copyFromUtf8("RawKV");

    // put
    client.put(key, value);

    // get
    Optional<ByteString> result = client.get(key);
    assert(result.isPresent());
    assert("RawKV".equals(result.get().toStringUtf8()));
    System.out.println(result.get().toStringUtf8());

    // delete
    client.delete(key);

    // get
    result = client.get(key);
    assert(!result.isPresent());

    // close
    client.close();
    session.close();
  }
}
