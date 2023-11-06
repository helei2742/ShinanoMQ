package cn.com.shinano.ShinanoMQ.base.nettyhandler;

import cn.com.shinano.ShinanoMQ.base.Message;
import io.netty.channel.ChannelHandlerContext;

/**
 * 规定里netty handler 具有的行为
 */
public interface NettyBaseHandler {

   /**
    * 发送消息
    * @param context context
    * @param msg 消息体
    */
   void sendMsg(ChannelHandlerContext context,Message msg);

   /**
    * 打印日志
    * @param logStr 日志字符串
    */
   void printLog(String logStr);
}
