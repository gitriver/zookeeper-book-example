package org.apache.zookeeper.book;

import java.util.Date;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


/**
 *
 */
public class AdminClient implements Watcher {
    ZooKeeper zk;
    String hostPort;


    AdminClient(String hostPort) {
        this.hostPort = hostPort;
    }


    void start() throws Exception {
        zk = new ZooKeeper(hostPort, 15000, this);
    }


    void listState() throws KeeperException, InterruptedException{
        try{
            Stat stat = new Stat();
            byte[] masterData = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master: "+new String(masterData)+" Since: "+startDate);
        }catch(NoNodeException e){
            System.out.println("no master");
        }
        
        System.out.println("Workers:");
        for(String w:zk.getChildren("/workers", false)){
            byte[] data = zk.getData("/workers/"+w, false, null);
            System.out.println(new String(data));
        }
        
        System.out.println("Tasks:");
        for(String t:zk.getChildren("/tasks", false)){
            byte[] data = zk.getData("/tasks/"+t, false, null);
            System.out.println(new String(data));
        }
    }


    @Override
    public void process(WatchedEvent e) {
        System.out.println(e);

    }
    
    public static void main(String[] args) throws Exception {
        AdminClient ac = new AdminClient("localhost:2181");
        ac.start();
        ac.listState();
    }

}
