package com.im.kafka_storm.utils;

import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;




public class LockCuratorSrc {

	private static CuratorFramework client = null;
	
	public synchronized  static CuratorFramework getCF(){
		if(client==null){
			try {
				RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
				System.out.println("create client--------");
				client = CuratorFrameworkFactory.newClient(Constant.ZK_HOST_PORT, retryPolicy);
				client.start();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return client;
	}
	
	public static void handleLockData(int index)throws Exception{
		//getCuratorFramework().create().forPath("/locks/2");
		InterProcessMutex lock = new InterProcessMutex(getCF(), "/locks/2");
		while (lock.acquire(1, TimeUnit.MINUTES) ) 
		{
		    try 
		    {
		        // do some work inside of the critical section here
		    	System.out.println("--------index = "+index+"-------------");
		    }
		    finally
		    {
		        lock.release();
		        break;
		    }
		}
	}
	
	public static void main(String[] args)throws Exception{
		//handleLockData(1);
		try{
			for(int i=0 ;i<10;i++){
				final int index = i;
				Runnable task1 = new Runnable(){
		            public void run() {
		            	System.out.println("-------------------------------");
						try {
							handleLockData(index);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
		            }
				};
				new Thread(task1).start();
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
}
