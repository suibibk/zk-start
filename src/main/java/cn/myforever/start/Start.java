package cn.myforever.start;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import cn.myforever.utils.ZKUtil;

/**
 * zookeeper连接
 * @author Administrator
 *
 */
public class Start{
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		ZKUtil zk = new ZKUtil();
		try {
			zk.openConnect();
			System.out.println("-------");
			zk.createPERSISTENT_SEQUENTIAL("/testS", "testS1");
			zk.createPERSISTENT_SEQUENTIAL("/testS2", "testS12");
			zk.delete("/test");
			zk.createEPHEMERAL("/test", "test");
			//临时节点断开后就会失效
			Thread.sleep(10000);
			zk.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

