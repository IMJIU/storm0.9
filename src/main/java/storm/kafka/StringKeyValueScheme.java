package storm.kafka;

import backtype.storm.tuple.Values;
import com.google.common.collect.ImmutableMap;

import java.util.List;

public class StringKeyValueScheme extends StringScheme implements KeyValueScheme {

    @Override
    public List<Object> deserializeKeyAndValue(byte[] key, byte[] value) {
        if ( key == null ) {
        	System.out.println("key nuu:"+value);
            return deserialize(value);
        }
        String keyString = StringScheme.deserializeString(key);
        System.out.println("key:"+keyString);
        String valueString = StringScheme.deserializeString(value);
        System.out.println(keyString);
        return new Values(ImmutableMap.of(keyString, valueString));
    }

}
