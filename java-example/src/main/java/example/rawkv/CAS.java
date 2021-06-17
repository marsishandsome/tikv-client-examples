package example.rawkv;

import java.util.Optional;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

public class CAS {
  public static void main(String[] args) throws Exception {
    TiConfiguration conf = TiConfiguration.createRawDefault("127.0.0.1:2379");
    // enable AtomicForCAS when using RawKVClient.compareAndSet or RawKVClient.putIfAbsent
    conf.setEnableAtomicForCAS(true);
    TiSession session = TiSession.create(conf);
    RawKVClient client = session.createRawClient();

    ByteString key = ByteString.copyFromUtf8("Hello");
    ByteString value = ByteString.copyFromUtf8("CAS");
    ByteString newValue = ByteString.copyFromUtf8("NewValue");

    // put
    client.put(key, value);
    System.out.println("put key=" + key.toStringUtf8() + " value=" + value.toStringUtf8());

    // get
    Optional<ByteString> result = client.get(key);
    assert(result.isPresent());
    assert("CAS".equals(result.get().toStringUtf8()));
    System.out.println("get key=" + key.toStringUtf8() + " result=" + result.get().toStringUtf8());

    // cas
    client.compareAndSet(key, Optional.of(value), newValue);
    System.out.println("cas key=" + key.toStringUtf8() + " value=" + value.toStringUtf8() + " newValue=" + newValue.toStringUtf8());

    // get
    result = client.get(key);
    assert(result.isPresent());
    assert("NewValue".equals(result.get().toStringUtf8()));
    System.out.println("get key=" + key.toStringUtf8() + " result=" + result.get().toStringUtf8());

    // close
    client.close();
    session.close();
  }
}
