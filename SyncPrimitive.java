import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.Arrays;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.util.Scanner;
import java.util.Timer;
import java.util.Date;
import java.nio.charset.StandardCharsets;
import java.nio.charset.Charset;

public class SyncPrimitive implements Watcher {

    static ZooKeeper zk = null;
    static Integer mutex;

    String root;

    SyncPrimitive(String address) {
        if(zk == null){
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, 3000, this);
                mutex = new Integer(-1);
                System.out.println("Finished starting ZK: " + zk);
            } catch (IOException e) {
                System.out.println(e.toString());
                zk = null;
            }
        }
        //else mutex = new Integer(-1);
    }

    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            System.out.println("Rodei Process: " + event.getType());
            mutex.notify();
        }
    }

    public void writeInNode(String node, String info) throws KeeperException, InterruptedException {
        //System.out.println("Escrevento "+info+" em "+node);
        Stat s = zk.exists(node, false);
        if (s == null) {
            zk.create(node, info.getBytes(StandardCharsets.UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }else{
            Stat stat = new Stat();
            zk.getData(node, null, stat);
            zk.setData(node, info.getBytes(StandardCharsets.UTF_8), stat.getVersion());
        }
    }

    /**
     * Barrier
     */
    static public class Barrier extends SyncPrimitive {
        int size;
        String name;

        /**
         * Barrier constructor
         *
         * @param address
         * @param root
         * @param size
         */
        Barrier(String address, String root, int size) {
            super(address);
            this.root = root;
            this.size = size;

            // Create barrier node
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out
                            .println("Keeper exception when instantiating queue: "
                                    + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }

            // My node name
            try {
                name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
            } catch (UnknownHostException e) {
                System.out.println(e.toString());
            }

        }

        /**
         * Join barrier
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */

        boolean enter() throws KeeperException, InterruptedException{
            zk.create(root + "/" + name, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);

                    if (list.size() < size) {
                        mutex.wait();
                    } else {
                        return true;
                    }
                }
            }
        }

        /**
         * Wait until all reach barrier
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */

        boolean leave() throws KeeperException, InterruptedException{
            String statLeilao = "2";//indica status 2 do leilao (fim barrier)
            try{
                System.out.println("Tentando sair barreira... " ); 
                byte[] b = zk.getData("/statLeilaoOn",false, null);//checa valor no status leilao
                String statLeilaoAtual = new String(b, "UTF-8");  //converte para inteiro
                if(Integer.parseInt(statLeilao) == 2){//se status leilao eh 2 entao outro cliente que saiu da barrier ja alterou o status leilao, logo nao eh preciso alterar mais
                    super.writeInNode("/statLeilaoOn", statLeilao) ;//atualiza status dentro no leilao para 2 (fim barrier)
                    //b = zk.getData("/statLeilaoOn",false, null);
                    //statLeilao = new String(b, "UTF-8");
                    //System.out.println("Alterando no leilao para: " + statLeilao); 
                }
            } catch (Exception e){
                e.printStackTrace();
            }
            zk.delete(root + "/" + name, 0);
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                        if (list.size() > 0) {
                            mutex.wait();
                        } else {
                            return true;
                        }
                    }
                }
        }
    }

    /**
     * Producer-Consumer queue
     */
    static public class Queue extends SyncPrimitive {

        /**
         * Constructor of producer-consumer queue
         *
         * @param address
         * @param name
         */
        Queue(String address, String name) {
            super(address);
            this.root = name;
            // Create ZK node name
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out.println("Keeper exception when instantiating queue: " + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }
        }

        /**
         * Add element to the queue.
         *
         * @param i
         * @return
         */

        boolean produce(int i) throws KeeperException, InterruptedException{
            ByteBuffer b = ByteBuffer.allocate(4);
            byte[] value;

            // Add child with value i
            b.putInt(i);
            value = b.array();
            zk.create(root + "/element", value, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

            return true;
        }


        /**
         * Remove first element from the queue.
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */

        int getListSize () throws KeeperException, InterruptedException{
            List<String> list = zk.getChildren(root, false);
            return list.size();
        }
        int consume() throws KeeperException, InterruptedException{
            int retvalue = -1;
            Stat stat = null;

            // Get the first element available
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                    if (list.size() == 0) {
                        System.out.println("Going to wait");
                        mutex.wait();
                    } else {
                        Integer min = new Integer(list.get(0).substring(7));
                        //System.out.println("List: "+list.toString());
                        String minString = list.get(0);
                        for(String s : list){
                            Integer tempValue = new Integer(s.substring(7));
                            //System.out.println("Temp value: " + tempValue);
                            if(tempValue < min) { 
                            	min = tempValue;
                            	minString = s;
                            }
                        }
                       //System.out.println("Temporary value: " + root +"/"+ minString);
                        byte[] b = zk.getData(root +"/"+ minString,false, stat);
                        //System.out.println("b: " + Arrays.toString(b)); 	
                        zk.delete(root +"/"+ minString, 0);
                        ByteBuffer buffer = ByteBuffer.wrap(b);
                        retvalue = buffer.getInt();
                        return retvalue;
                    }
                }
            }
        }
    }

    static public class Lock extends SyncPrimitive {
    	long wait;
	String pathName;
    	 /**
         * Constructor of lock
         *
         * @param address
         * @param name Name of the lock node
         */
        Lock(String address, String name, long waitTime) {
            super(address);
            this.root = name;
            this.wait = waitTime;
            // Create ZK node name
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out.println("Keeper exception when instantiating queue: " + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }
        }
        
        boolean lock() throws KeeperException, InterruptedException{
            //Step 1
            pathName = zk.create(root + "/lock-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("My path name is: "+pathName);
            //Steps 2 to 5
            return testMin();
        }
        
	boolean testMin() throws KeeperException, InterruptedException{
	    while (true) {
		 Integer suffix = new Integer(pathName.substring(12));
	         //Step 2 
            	 List<String> list = zk.getChildren(root, false);
                 Integer min = new Integer(list.get(0).substring(5));
                 System.out.println("List: "+list.toString());
                 String minString = list.get(0);
                 for(String s : list){
                 	Integer tempValue = new Integer(s.substring(5));
                 	//System.out.println("Temp value: " + tempValue);
                 	if(tempValue < min)  {
                 		min = tempValue;
                 		minString = s;
                 	}
                 }
                System.out.println("Suffix: "+suffix+", min: "+min);
           	//Step 3
             	if (suffix.equals(min)) {
            		System.out.println("Lock acquired for "+minString+"!");
            		return true;
            	}
            	//Step 4
            	//Wait for the removal of the next lowest sequence number
            	Integer max = min;
            	String maxString = minString;
            	for(String s : list){
            		Integer tempValue = new Integer(s.substring(5));
            		//System.out.println("Temp value: " + tempValue);
            		if(tempValue > max && tempValue < suffix)  {
            			max = tempValue;
            			maxString = s;
            		}
            	}
            	//Exists with watch
            	Stat s = zk.exists(root+"/"+maxString, this);
            	System.out.println("Watching "+root+"/"+maxString);
            	//Step 5
            	if (s != null) {
            	    //Wait for notification
            	    break;  
            	}
	    }
            System.out.println(pathName+" is waiting for a notification!");
	    return false;
	}

        synchronized public void process(WatchedEvent event) {
            synchronized (mutex) {
            	String path = event.getPath();
            	if (event.getType() == Event.EventType.NodeDeleted) {
            		System.out.println("Notification from "+path);
			try {
			    if (testMin()) { //Step 5 (cont.) -> go to step 2 to check
				this.compute();
			    } else {
				System.out.println("Not lowest sequence number! Waiting for a new notification.");
			    }
			} catch (Exception e) {e.printStackTrace();}
            	}
            }
        }
        
        void compute() {
        	System.out.println("Lock acquired!");
    		try {
				new Thread().sleep(wait);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		//Exits, which releases the ephemeral node (Unlock operation)
    		System.out.println("Lock released!");
    		System.exit(0);
        }
    }

    


    static public class Leader extends SyncPrimitive{
    	String leader;
    	String id; //Id of the leader
    	String pathName;
        String address;
    	
   	 /**
         * Constructor of Leader
         *
         * @param address
         * @param name Name of the election node
         * @param leader Name of the leader node
         * 
         */
        Leader(String address, String name, String leader, int id) {
            super(address);
            this.address = address;
            this.root = name;
            this.leader = leader;
            this.id = new Integer(id).toString();
            // Create ZK node name
            if (zk != null) {
                try {
                	//Create election znode
                    Stat s1 = zk.exists(root, false);
                    if (s1 == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }  
                    //Checking for a leader
                    Stat s2 = zk.exists(leader, false);
                    if (s2 != null) {
                        byte[] idLeader = zk.getData(leader, false, s2);
                        System.out.println("Current leader with id: "+new String(idLeader));
                    }  
                    
                } catch (KeeperException e) {
                    System.out.println("Keeper exception when instantiating queue: " + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        
        boolean elect() throws KeeperException, InterruptedException{
        	this.pathName = zk.create(root + "/n-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        	System.out.println("My path name is: "+pathName+" and my id is: "+id+"!");
        	return check();
        }
        
        boolean check() throws KeeperException, InterruptedException{
        	Integer suffix = new Integer(pathName.substring(12));
           	while (true) {
        		List<String> list = zk.getChildren(root, false);
        		Integer min = new Integer(list.get(0).substring(5));
        		System.out.println("List: "+list.toString());
        		String minString = list.get(0);
        		for(String s : list){
        			Integer tempValue = new Integer(s.substring(5));
        			//System.out.println("Temp value: " + tempValue);
        			if(tempValue < min)  {
        				min = tempValue;
        				minString = s;
        			}
        		}
        		System.out.println("Suffix: "+suffix+", min: "+min);
        		if (suffix.equals(min)) {
        			this.leader();
        			return true;
        		}
        		Integer max = min;
        		String maxString = minString;
        		for(String s : list){
        			Integer tempValue = new Integer(s.substring(5));
        			//System.out.println("Temp value: " + tempValue);
        			if(tempValue > max && tempValue < suffix)  {
        				max = tempValue;
        				maxString = s;
        			}
        		}
        		//Exists with watch
        		Stat s = zk.exists(root+"/"+maxString, this);
        		System.out.println("Watching "+root+"/"+maxString);
        		//Step 5
        		if (s != null) {
        			//Wait for notification
        			break;
        		}
        	}
        	System.out.println(pathName+" is waiting for a notification!");
        	return false;
        	
        }
        
        synchronized public void process(WatchedEvent event) {
            synchronized (mutex) {
            	if (event.getType() == Event.EventType.NodeDeleted) {
            		try {
            			boolean success = check();
            			if (success) {
            				compute();
            			}
            		} catch (Exception e) {e.printStackTrace();}
            	}
            }
        }
        
        void leader() throws KeeperException, InterruptedException {
			System.out.println("Become a leader: "+id+"!");
            //Create leader znode
            Stat s2 = zk.exists(leader, false);
            if (s2 == null) {
                zk.create(leader, id.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } else {
            	zk.setData(leader, id.getBytes(), 0);
            }
        }

        
        void compute() {//nessa funcao o fluxo principal do lider ocorre
    		//System.out.println("Aqui irei realizar operacoes de lider!");
            
            String statNo = "/statLeilaoOn";//no que guarda status do leilao
            String statLeilao = "1";//status 'Em espera da barrier terminar ...' do leilao
            String valuesOffered = "/valuesLeilaoOn";//no que guardara maior valor ate o momento
            int valueProposed = 0;//variavel que guardara valores lidos na fila
            try{
                super.writeInNode(valuesOffered, Integer.toString(valueProposed));//inicia no que guarda maior valor leilao
                super.writeInNode(statNo, statLeilao);//inicia no que guarda status leilao
                do{//nesse loop eh aguardado o barrier terminar
                    byte[] b = zk.getData(statNo,false, null);//checando valor no status leilao
                    statLeilao = new String(b, "UTF-8");
                    //System.out.println("Aguardando para fila... Status leilao: " + statLeilao + " " + (statLeilao != "2")); 	
                }while(Integer.parseInt(statLeilao) != 2);//loop termina quando status leilao eh alocado para 2 pelo termino de algum cliente na barrier
            } catch (KeeperException e1){
                e1.printStackTrace();
            } catch (InterruptedException e1){
                e1.printStackTrace();
            } catch (Exception e1){
                e1.printStackTrace();
            }

            

            //System.out.println("Iniciando leitura fila");

            

            Queue q = new Queue(this.address, "/fila");//fila declarada
      
            long lastSetTime = System.currentTimeMillis();;//variavel que guardara ultimo milisegundo q algum valor entrou na fila
            long nowTime;//variavel que guarda valor atual na fila

            
            byte[] bs;//variavel para realizar operacoes de leitura com bytes
            int valueMaxCurrent = 0;//variavel que guardara maior valor ate momento

            while (true) {//processos de leitura e escrita valores ocorrem nesse loop
                try{   
                    nowTime =  System.currentTimeMillis();//atualiza-se o tempo atual
                    //System.out.println("Loop " + (nowTime - lastSetTime));
                    if((nowTime - lastSetTime) >= (20*1000)){//checa se ja passou mais do que 20 segundos desde o ultimo valor inserido na fila
                        //System.out.println("Fim leilao ");
                        break;//fim leilao
                    }
                    if(q.getListSize() > 0){//checa se ha valores na fila para serem lidos
                        valueProposed = q.consume() ; //Coleta valor presente na fila
                        //System.out.println("Thread - Item: " + valueProposed);

                        bs = zk.getData(valuesOffered,false, null);//le maior valor dentro do no de maior valor
                        valueMaxCurrent = Integer.parseInt(new String(bs, "UTF-8"));//converte para ijt
                        if (valueMaxCurrent < valueProposed){//compara maior valor ate o momento com valor retirado da fila
                            //System.out.println("Thread - Novo max: " + valueProposed);
                            super.writeInNode(valuesOffered, Integer.toString(valueProposed));//rescreve no de maior valor
                        }

                        lastSetTime = System.currentTimeMillis();//atualiza ultimo milisegundo em que houve valor na fila
                        
                    }
                    
                } catch (KeeperException e){
                    //System.out.println("Thread - Fila sem items ");
                    continue;
                } catch (InterruptedException e){
			        e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            try{  //operacoes para termino leilao
                statLeilao = "0";//status 0 indica fim leilao
                super.writeInNode(statNo, statLeilao) ;//atualiza no status
                bs = zk.getData(valuesOffered,false, null);//no de valor é lido (nele esta maior valor ate momento)
                valueMaxCurrent = Integer.parseInt(new String(bs, "UTF-8"));//converte para int
                super.writeInNode("/leilaoFim", Integer.toString(valueMaxCurrent));// novo no eh criado apenas para armazenar maior valor final do leilao (valor vencedor)
            } catch (KeeperException e){
                e.printStackTrace();
            } catch (InterruptedException e){
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static public class Mananger extends Thread{

        String serverAddress;

        Mananger(String serverAddress){
            this.serverAddress = serverAddress;
        }

        public void run(){
            leaderElection(this.serverAddress);
        }

        public static void leaderElection(String serverAddress) {
            // Gera lider
            Random rand = new Random();
            int r = rand.nextInt(1000000);
            Leader leader = new Leader(serverAddress,"/election","/leader",r);
            try{
                boolean success = leader.elect();
                if (success) {
                    leader.compute();//realiza operacoes de lider
                } else {
                    System.out.println("Nao sou lider");
                    while(true) {
                    //Waiting for a notification
                    }
                }         
            } catch (KeeperException e){
                e.printStackTrace();
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
    
    


    public static void main(String args[]) {

      

        Mananger mananger = new Mananger(args[0]);//manager que relizara operacoes de lider
        Integer minParticipants = new Integer(2);//quantidade minima de participantes no leilao
        Integer listLimit = new Integer(1);
        
        mananger.start();//inicia thread do fluxo de lider

        System.err.println("Iniciando leilão ");
        waitBarrier(args[0],minParticipants);//espera completar participantes pra desbloquear barrier
        produceValues(args[0],listLimit); //inicia leilao

    }

    public static void produceValues(String addressName, Integer listLimit){


        System.out.println("Entrando no leilao... ");

        String valuesOffered = "/valuesLeilaoOn";//no que guardara maior valor ate o momento
        String statLeilao = "/statLeilaoOn";//no que guarda status do leilao

        Queue q = new Queue(addressName, "/fila");//declara fila
        Lock lock = new Lock(addressName,"/lock",new Long(10000));//declara lock (nao esta sendo suado ainda)
        Scanner s = new Scanner(System.in);//declara scanner pra receber valores usuarios
        Integer valueToProduce = 0 ;//variavel que guardara valores que usuario insere na fila
        byte[] bs;//array de bytes para fazer operacoes de leitura dos nos
        int valueMaxCurrent = 0;//variavel que guarda maior valor ate momento

        while(true){//operacoes do cliente ocorrem aqui
            try{
                bs = zk.getData(statLeilao,false, null);//le status leilao
                int status = Integer.parseInt(new String(bs, "UTF-8"));//converte status em int
                if(status == 0) break;//se status for zero leilao acabou
                
                while( valueMaxCurrent <  valueToProduce){//espera no de maior valor atualizar
                    bs = zk.getData(valuesOffered,true, null);//le no que contem maior valor ate momento
                    valueMaxCurrent = Integer.parseInt(new String(bs, "UTF-8"));//converte em int
                }
                
                System.out.println("Maior valor no momento:" + valueMaxCurrent);//exibe maior valor
                System.out.println("Insira proximo valor leilao:");//pede input
                valueToProduce = new Integer(s.nextInt());//recebe input de proximo valor
                
                q.produce(valueToProduce);//insere input na fila
                
            } catch (KeeperException e){
                e.printStackTrace();
            } catch (InterruptedException e){
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
            
        }


        
        try{
            bs = zk.getData("/leilaoFim",false, null);//checa no que contem maior valor final do leilao (esse no eh atualizado pelo lider ao fim do leilao)
            valueMaxCurrent = Integer.parseInt(new String(bs, "UTF-8"));//converte em int
        }catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Fim leilao, valor vencedor foi:" + valueMaxCurrent);//exibe maior valor final do leilao
        

    }



    

    public static void waitBarrier(String serverAddress,Integer minParticipants) {
        System.out.println("Aguardando completar numero minimo de participantes... ");
        Barrier b = new Barrier(serverAddress, "/b1", minParticipants);
        try{
            boolean flag = b.enter();
            System.out.println("Entrei na barreira: " + minParticipants);
            if(!flag) System.out.println("Erro na barreira");
        } catch (KeeperException e){

        } catch (InterruptedException e){

        }

        // Generate random integer
        Random rand = new Random();
        int r = rand.nextInt(100);
        // Loop for rand iterations
        for (int i = 0; i < r; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
        }
        try{
            b.leave();
        } catch (KeeperException e){

        } catch (InterruptedException e){

        }
        System.out.println("Sai da barreira");
    }
    
    public static void lockTest(String args[]) {
    	Lock lock = new Lock(args[0],"/lock",new Long(10000));
        try{
        	boolean success = lock.lock();
        	if (success) {
        		lock.compute();
        	} else {
        		while(true) {
        			//Waiting for a notification
        		}
            }         
        } catch (KeeperException e){
        	e.printStackTrace();
        } catch (InterruptedException e){
        	e.printStackTrace();
        }
    }
    
}
