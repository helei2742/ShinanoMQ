package cn.com.shinano.ShinanoMQ.core.web.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lhe.shinano
 * @date 2023/10/13
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Result {
    private String msg;
    private Integer code;
    private Object data;

    public static Result fail(ResultCode code) {
        return fail(code, code.getMsg());
    }
    public static Result fail(ResultCode code, String msg) {
        return new Result(msg, code.value, null);
    }

    public static Result ok() {
        return new Result(ResultCode.SUCCESS.getMsg(), ResultCode.SUCCESS.value, null);
    }

    public static Result ok(Object list) {
        return new Result(ResultCode.SUCCESS.getMsg(), ResultCode.SUCCESS.value, list);
    }


    public enum ResultCode{
        SUCCESS(200),
        PARAMS_ERROR(501),
        TOPIC_CREATE_ERROR(502),
        TOPIC_CLOSE_ERROR(503),
        TOPIC_DELETE_ERROR(504),
        TOPIC_RECOVER_ERROR(505)
        ;

        static Map<ResultCode, String> msgMap;

        static {
            msgMap = new HashMap<>();
            msgMap.put(SUCCESS, "请求成功");
            msgMap.put(PARAMS_ERROR, "参数错误");
            msgMap.put(TOPIC_CREATE_ERROR, "创建topic失败");
            msgMap.put(TOPIC_CLOSE_ERROR, "关闭topic失败");
            msgMap.put(TOPIC_DELETE_ERROR, "删除topic失败");
            msgMap.put(TOPIC_RECOVER_ERROR, "恢复topic失败");
        }

        ResultCode(Integer value) {
            this.value = value;
        }
        private Integer value;

        public Integer getValue() {
            return value;
        }
        public String getMsg() {
            return msgMap.get(this);
        }
    }
}
