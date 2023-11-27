package cn.com.shinano.ShinanoMQ.base.constant;

/**
 * @author lhe.shinano
 * @date 2023/11/27
 */
public enum LoadBalancePolicy {
    ROUND_ROBIN("ROUND_ROBIN"),
    RANDOM("RANDOM"),
    LEAST_CONNECTIONS("LEAST_CONNECTIONS"),
    IP_HASH("IP_HASH"),
    WEIGHTED_ROUND_ROBIN("WEIGHTED_ROUND_ROBIN"),
    WEIGHTED_RANDOM("WEIGHTED_RANDOM"),
    LEAST_RESPONSE_TIME("LEAST_RESPONSE_TIME");

    public final String value;
    private LoadBalancePolicy(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }
}
