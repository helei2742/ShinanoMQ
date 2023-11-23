package cn.com.shinano.ShinanoMQ.base.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class Pair<K,V> {
    private K key;
    private V value;
}
