package com.xiaoyu.demo.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

/**
 * JAVA语言操作ZK
 * @author xiaoyu
 */
public class ZookeeperTest implements Watcher {
    //ZK连接地址
    private static final String CONNECTSTRING = "127.0.0.1:2181";
    //会话超时时间
    private static final int SESSION_TIMEOUT = 2000;
    //使用countDownLatch阻塞用户程序，用户必须等待连接，发送成功信号
    private static final CountDownLatch countDownLatch = new CountDownLatch(1);

    private ZooKeeper zooKeeper;

    /**
     * 创建连接
     * @param connectString 连接地址
     * @param sessionTimeout 会话超时
     */
    public void createConnection(String connectString,int sessionTimeout){
        try {
            zooKeeper =  new ZooKeeper(connectString,sessionTimeout,this);
            countDownLatch.countDown();
            System.out.println("-------------开始建立连接---------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 实现了Watcher接口后，Zookeeper的节点发生改变后会调用该监听方法
     * @param watchedEvent
     */
    public void process(WatchedEvent watchedEvent) {
        System.out.println();
        System.out.println("----【proccess】事件通知开始----");
        //获取事件状态
        Event.KeeperState state = watchedEvent.getState();
        //获取节点路径
        String path = watchedEvent.getPath();
        //获取事件类型
        Event.EventType type = watchedEvent.getType();
        System.out.println("----【proceses】进入监听方法：state = "+state+", path = "+path+", eventType = "+type+"----");
        //判断为连接状态
        if(Event.KeeperState.SyncConnected == state){
            //根据不同的事件类型输出不同的语句
            if(Event.EventType.None == type){
                countDownLatch.countDown();
            }else if(Event.EventType.NodeCreated == type){
                System.out.println("--------【proceses】事件通知，当前节点"+path+"新增成功----");
            }else if(Event.EventType.NodeDataChanged == type){
                System.out.println("--------【proceses】事件通知，当前节点"+path+"修改成功----");
            }else if(Event.EventType.NodeDeleted == type){
                System.out.println("--------【proceses】事件通知，当前节点"+path+"删除成功----");
            }
        }
        System.out.println("----【process】事件通知结束----");
        System.out.println();
    }

    /**
     * 创建节点
     * @param path 节点路径
     * @param data 节点数据
     * @return
     */
    public boolean createNode(String path,String data){
        /**
         * 创建持久节点
         */
        try {
            Stat exists = exists(path, true);
            String result = zooKeeper.create(path,data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("新增节点成功: path = "+path+", data = "+data+", result = "+result);
            return true;
        } catch (Exception e) {
            System.out.println("新增节点失败:"+e.getMessage());
            return false;
        }
    }


    /**
     * 修改节点数据
     * @param path 节点路径
     * @param data 新数据
     * @return
     */
    public boolean updateNode(String path,String data){
        try {
            Stat exists = exists(path, true);
            Stat result = zooKeeper.setData(path, data.getBytes(), -1);
            System.out.println("修改节点成功：path = "+path+", data = "+data+", result = "+result);
            return true;
        } catch (Exception e) {
            System.out.println("修改节点失败: "+e.getMessage());
            return false;
        }
    }

    /**
     * 关闭Zookeeper连接
     */
    public void close(){
        try {
            if(zooKeeper != null){
                zooKeeper.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 为节点设置是否需要发布事件通知
     *
     * 如果未调用该方法，那么节点事件将不会发布
     * @param path 节点路径
     * @param enableWatcher  是否开启事件通知 true/false
     * @return
     */
    public Stat exists(String path,boolean enableWatcher){
        try {
           return zooKeeper.exists(path,enableWatcher);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        //创建测试类实例
        ZookeeperTest test = new ZookeeperTest();
        //创建连接
        test.createConnection(CONNECTSTRING,SESSION_TIMEOUT);
        //新增节点
        test.createNode("/Node1/Node2/Node3","Data3");
        //更新节点
//        test.updateNode("/Node1/Node2","Data3");
        //关闭Zookeeper连接
        test.close();
    }
}
