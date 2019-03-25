package cn.myforever.utils;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * zookeeper操作工具类
 * @author Administrator
 *
 */
public class ZKUtil implements Watcher{
	//等待多线程完成的工具类
	private static final CountDownLatch cdl = new CountDownLatch(1);
	//zk服务器连接地址
	private static final String connectString = "127.0.0.1:2181";
	//zk服务器连接超时时间
	private static final Integer sessionTimeout = 5000;
	private ZooKeeper zk;
	/**
	 * 打开连接
	 * @throws Exception
	 */
	public void openConnect() throws Exception {
		System.out.println("开始连接zk");
		zk =new ZooKeeper(connectString, sessionTimeout,this);
		cdl.await();
	}
	/**
	 * 关闭连接
	 * @throws Exception
	 */
	public void close() throws Exception {
		if(zk!=null) {
			zk.close();
		}
	}
	/**
	 * 创建持久话节点
	 * @param path 节点路径
	 * @param data 节点内容
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String createPERSISTENT(String path,String data) throws KeeperException, InterruptedException {
		Stat stat = zk.exists(path, true);
		System.out.println("createPERSISTENT"+stat);
		if(stat==null) {
			return zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}else {
			System.out.println("节点"+path+"已经存在不可以再插入");
			return "";
		}
	} 
	/**
	 * 创建持久话顺序节点
	 * @param path 节点路径
	 * @param data 节点内容
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String createPERSISTENT_SEQUENTIAL(String path,String data) throws KeeperException, InterruptedException {
		Stat stat = zk.exists(path, true);
		System.out.println("createPERSISTENT_SEQUENTIAL"+stat);
		if(stat==null) {
			return zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		}else {
			System.out.println("节点"+path+"已经存在不可以再插入");
			return "";
		}
	} 
	/**
	 * 创建临时节点
	 * @param path 节点路径
	 * @param data 节点内容
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String createEPHEMERAL(String path,String data) throws KeeperException, InterruptedException {
		Stat stat = zk.exists(path, true);
		System.out.println("createEPHEMERAL"+stat);
		if(stat==null) {
			return zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		}else {
			System.out.println("节点"+path+"已经存在不可以再插入");
			return "";
		}
	}
	/**
	 * 创建临时节点
	 * @param path 节点路径
	 * @param data 节点内容
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String createEPHEMERAL_SEQUENTIAL(String path,String data) throws KeeperException, InterruptedException {
		Stat stat = zk.exists(path, true);
		System.out.println("createEPHEMERAL_SEQUENTIAL"+stat);
		if(stat==null) {
			return zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		}else {
			System.out.println("节点"+path+"已经存在不可以再插入");
			return "";
		}
	}
	/**
	 * 更新节点
	 * @param path  节点路径
	 * @param data  节点内容
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public Stat update(String path,String data) throws KeeperException, InterruptedException {
		Stat stat = zk.exists(path, true);
		System.out.println("update"+stat);
		if(stat!=null) {
			return zk.setData(path, data.getBytes(), -1);
		}else {
			System.out.println("节点"+path+"不存在，不可以更新");
			return null;
		}
		
	}
	/**
	 * 删除节点
	 * @param path   节点路径
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void delete(String path) throws InterruptedException, KeeperException {
		Stat stat = zk.exists(path, true);
		System.out.println("update"+stat);
		if(stat!=null) {
			zk.delete(path, -1);
		}else {
			System.out.println("节点"+path+"不存在，不可以删除");
		}
	}
	public void process(WatchedEvent event) {
		KeeperState state =event.getState();
		if(state==KeeperState.SyncConnected) {
			EventType type =event.getType();
			if(type==EventType.None) {
				System.out.println("链接zk成功");
				cdl.countDown();
			}else {
				System.out.println("zkType:"+type);
			}
		}
	}

}

