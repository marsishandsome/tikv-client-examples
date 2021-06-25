package example.rawkv;

import java.util.Optional;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

/**
 * TTL is disabled by default. Create a file `tikv.yaml` using the following content to enable TTL.
 *
 * [storage]
 * enable-ttl = true
 *
 * tiup playground --kv.config tikv.yaml
 */
public class TTL {
  public static void main(String[] args) throws Exception {
    ttl();
    casWithTTL();
  }

  private static void ttl() throws Exception {
    TiConfiguration conf = TiConfiguration.createRawDefault("127.0.0.1:2379");
    TiSession session = TiSession.create(conf);
    RawKVClient client = session.createRawClient();

    // write (k1, v1) with ttl=10 seconds
    client.put(ByteString.copyFromUtf8("k1"), ByteString.copyFromUtf8("v1"), 10);

    // write (k2, v2) without ttl
    client.put(ByteString.copyFromUtf8("k2"), ByteString.copyFromUtf8("v2"));

    // get k1 returns v1
    Optional<ByteString> result1 = client.get(ByteString.copyFromUtf8("k1"));
    assert(result1.isPresent());
    assert("v1".equals(result1.get().toStringUtf8()));
    System.out.println(result1.get().toStringUtf8());

    // get k2 returns v2
    Optional<ByteString> result2 = client.get(ByteString.copyFromUtf8("k2"));
    assert(result2.isPresent());
    assert("v2".equals(result2.get().toStringUtf8()));
    System.out.println(result2.get().toStringUtf8());

    // sleep 10 seconds
    System.out.println("Sleep 10 seconds.");
    Thread.sleep(10000);

    // get k1 returns null, cause k1's ttl is expired
    result1 = client.get(ByteString.copyFromUtf8("k1"));
    assert(!result1.isPresent());

    // get k2 returns v2
    result2 = client.get(ByteString.copyFromUtf8("k2"));
    assert(result2.isPresent());
    assert("v2".equals(result2.get().toStringUtf8()));
    System.out.println(result2.get().toStringUtf8());

    // close
    client.close();
    session.close();
  }

  private static void casWithTTL() throws Exception {
    TiConfiguration conf = TiConfiguration.createRawDefault("127.0.0.1:2379");
    // enable AtomicForCAS when using RawKVClient.compareAndSet or RawKVClient.putIfAbsent
    conf.setEnableAtomicForCAS(true);
    TiSession session = TiSession.create(conf);
    RawKVClient client = session.createRawClient();

    ByteString key = ByteString.copyFromUtf8("Hello");
    ByteString value = ByteString.copyFromUtf8("CAS+TTL");
    ByteString newValue = ByteString.copyFromUtf8("NewValue");

    // put
    client.put(key, value);

    // cas with ttl = 10 seconds
    client.compareAndSet(key, Optional.of(value), newValue, 10);

    // get
    Optional<ByteString> result = client.get(key);
    assert(result.isPresent());
    assert("NewValue".equals(result.get().toStringUtf8()));
    System.out.println(result.get().toStringUtf8());

    // sleep 10 seconds
    System.out.println("Sleep 10 seconds.");
    Thread.sleep(10000);

    // get
    result = client.get(key);
    assert(!result.isPresent());

    // close
    client.close();
    session.close();
  }
}
