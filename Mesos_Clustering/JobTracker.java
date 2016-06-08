import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.InputStream;  
import java.util.Properties;
import java.io.FileInputStream; 

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Filters;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value;



public class JobTracker{	
	static class JobScheduler implements Scheduler{		
	    public JobScheduler(ExecutorInfo executor, int totalTasks) {
	     	        
	        if(strategymode.equals("dynamic")){

	            this.executor = executor;
	            this.totalTasks = totalTasks;

	        	GlobalStrategy global = new GlobalStrategy();	      
	        	this.allpossibletaskspernodewithorder=global.getfinallist(datapernode, allfilename);	
	        	this.taskslistpernodewithorder=global.getkeyforeachmap(this.allpossibletaskspernodewithorder);

	        	workernodeandtaskconnectionlist=dynamicinitial(workernodelist,this.taskslistpernodewithorder);
	        	nodetaskindex=dynamicinitial2(workernodelist);
	            this.alltasks=global.GenerateAlltaskslist(allfilename);

	        }else if(strategymode.equals("datamining")){

	        	this.executor = executor;
	            this.totalTasks = workernodelist.size();

	        	nodetaskindex=dynamicinitial2(workernodelist);

	        }else{

	        	this.executor = executor;
	            this.totalTasks = totalTasks;

	            this.alltaskmap=SystemRun.preparetorunthree(allfilename,statictaskpernode);
	        	nodetaskindex=dynamicinitial2(workernodelist);
	        	workernodeandtaskconnectionforstatic=staticinitial(workernodelist,alltaskmap);
	     
	        }
	    }
	

	    @Override
	    public void registered(SchedulerDriver driver, 
	                           FrameworkID frameworkId, 
	                           MasterInfo masterInfo) {
	      System.out.println("Registered! ID = " + frameworkId.getValue());
	    }

	    @Override
	    public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {}

	    @Override
	    public void disconnected(SchedulerDriver driver) {}



	    public HashMap<String, ArrayList<ArrayList<String>>> dynamicinitial(ArrayList<String> inputworkernodelist, ArrayList<ArrayList<ArrayList<String>>> inputtaskslistpernodewithorder){

	    	HashMap<String, ArrayList<ArrayList<String>>> workernodeandtaskconnection = new HashMap<String, ArrayList<ArrayList<String>>>();


	    	for(int i=0;i<inputworkernodelist.size();i++){

	    		workernodeandtaskconnection.put(inputworkernodelist.get(i),inputtaskslistpernodewithorder.get(i));

	    	}

	    	return workernodeandtaskconnection;
	    }


	    public HashMap<String, Integer> dynamicinitial2(ArrayList<String> inputworkernodelist){

	    	HashMap<String, Integer> indexmap = new HashMap<String, Integer>();


	    	for(int i=0;i<inputworkernodelist.size();i++){

	    		indexmap.put(inputworkernodelist.get(i),0);

	    	}

	    	return indexmap;

	    }


	    public HashMap<String, List<List<String>>> staticinitial(ArrayList<String> inputworkernodelist, HashMap<Integer,List<List<String>>> alltaskmap){

			HashMap<String, List<List<String>>> workernodeandtaskconnection = new HashMap<String, List<List<String>>>();


			for(int i=0;i<inputworkernodelist.size();i++)
			workernodeandtaskconnection.put(inputworkernodelist.get(i),alltaskmap.get(i));

			return workernodeandtaskconnection;
	    }



	    
	    public void smartchoosedynamic(Offer offer,SchedulerDriver driver,ArrayList<ArrayList<String>> alltasks,HashMap<String, ArrayList<ArrayList<String>>> tasknodeconnection,HashMap<String, Integer> nodetaskindex){
	    	
	    	String Stringvalue;
	    	String Disturbname;
	    	ArrayList<ArrayList<String>> matchedtasklist;
	    	int thislength=0;

	    	String offerfromhostname;

	    	offerfromhostname=offer.getHostname();
	    	thislength=tasknodeconnection.get(offerfromhostname).size();

	    	if(((launchedTasks < totalTasks)&&(nodetaskindex.get(offerfromhostname)<thislength))){

	    		int temp=0;

		        temp=alltasks.indexOf(tasknodeconnection.get(offerfromhostname).get(nodetaskindex.get(offerfromhostname)));

		        	
		        		if(nodetaskindex.get(offerfromhostname)<thislength){


		        		if(!(temp<0))
		        		{
		        			
		        			 List<TaskInfo> tasks = new ArrayList<TaskInfo>();
		        			 	        			 
		        			 Stringvalue=choosemultipletask(tasknodeconnection.get(offerfromhostname), nodetaskindex.get(offerfromhostname), 1);
		        			        	
		 		          TaskID taskId = TaskID.newBuilder()
		 		        		  .setValue(Stringvalue).build();

		 		          System.out.println("Launching task " + taskId.getValue());		          	          
		 		         

		 		          TaskInfo task = TaskInfo.newBuilder()
		 		            .setName("Task " + taskId.getValue())
		 		            .setTaskId(taskId)
		 		            .setSlaveId(offer.getSlaveId())
		 		            .addResources(Resource.newBuilder()
		 		                          .setName("cpus")
		 		                          .setType(Value.Type.SCALAR)
		 		                          .setScalar(Value.Scalar.newBuilder()
		 		                                     .setValue(1)
		 		                                     .build())
		 		                          .build())
		 		            .addResources(Resource.newBuilder()
		 		                          .setName("mem")
		 		                          .setType(Value.Type.SCALAR)
		 		                          .setScalar(Value.Scalar.newBuilder()
		 		                                     .setValue(25000)
		 		                                     .build())
		 		                          .build())
		 		            .setExecutor(executor)
		 		            .build();
		 		          tasks.add(task);
		 		          
		 		          
		 		          
		 		           Filters filters = Filters.newBuilder().setRefuseSeconds(3).build();
		 		        driver.launchTasks(offer.getId(), tasks, filters);
		 		        
		 		        alltasks.remove(temp);

		 		        nodetaskindex.put(offerfromhostname,nodetaskindex.get(offerfromhostname)+1);

		 		        launchedTasks++;
		 		        	 		        
		 		        tasks=null;
		        			
		        		}
		        		else
		        		{
		        			nodetaskindex.put(offerfromhostname,nodetaskindex.get(offerfromhostname)+1);		        			
		        		}
		        	}

	    	}
	    }






	    public void smartchoosestatic(Offer offer,SchedulerDriver driver,HashMap<Integer,List<List<String>>> alltaskmap, HashMap<String, List<List<String>>> tasknodeconnection,HashMap<String, Integer> nodetaskindex){
	    	
	    	String Stringvalue;

	        int thislength=0;

	    	String offerfromhostname;

	    	offerfromhostname=offer.getHostname();
	    	thislength=tasknodeconnection.get(offerfromhostname).size();


	    	if(((launchedTasks < totalTasks)&&(nodetaskindex.get(offerfromhostname)<thislength))){

	        	
		        	 List<TaskInfo> tasks = new ArrayList<TaskInfo>();


		        	Stringvalue=tasknodeconnection.get(offerfromhostname).get(nodetaskindex.get(offerfromhostname)).get(0)+" "+tasknodeconnection.get(offerfromhostname).get(nodetaskindex.get(offerfromhostname)).get(1);

		        		        	
		          TaskID taskId = TaskID.newBuilder()
		        		  .setValue(Stringvalue).build();

		          System.out.println("Launching task " + taskId.getValue());		          	          
		         

		          TaskInfo task = TaskInfo.newBuilder()
		            .setName("Task " + taskId.getValue())
		            .setTaskId(taskId)
		            .setSlaveId(offer.getSlaveId())
		            .addResources(Resource.newBuilder()
		                          .setName("cpus")
		                          .setType(Value.Type.SCALAR)
		                          .setScalar(Value.Scalar.newBuilder()
		                                     .setValue(1)
		                                     .build())
		                          .build())
		            .addResources(Resource.newBuilder()
		                          .setName("mem")
		                          .setType(Value.Type.SCALAR)
		                          .setScalar(Value.Scalar.newBuilder()
		                                     .setValue(25000)
		                                     .build())
		                          .build())
		            .setExecutor(executor)
		            .build();
		          tasks.add(task);
		          
		          
		          
		           Filters filters = Filters.newBuilder().setRefuseSeconds(3).build();
		        driver.launchTasks(offer.getId(), tasks, filters);

		        nodetaskindex.put(offerfromhostname,nodetaskindex.get(offerfromhostname)+1);

		        launchedTasks++;
		        tasks=null;
		        
		         		          
		        }	      	
	    	
	    }




	    public void smartchoosedatamining(Offer offer,SchedulerDriver driver,HashMap<String, Integer> nodetaskindex){

	    	String Stringvalue;
	    	String offerfromhostname;

	    	offerfromhostname=offer.getHostname();

	    	if(nodetaskindex.get(offerfromhostname) < 1){
	        	
		        List<TaskInfo> tasks = new ArrayList<TaskInfo>();

	//	        Stringvalue=nodetaskindex.get(offerfromhostname)+" "+"Mining";
		        
		        
		        
		        Stringvalue=dataminingpath1+" "+offerfromhostname;
		        
		        		        	
		          TaskID taskId = TaskID.newBuilder()
		        		  .setValue(Stringvalue).build();

		          System.out.println("Launching task " + taskId.getValue());		          	          
		         

		          TaskInfo task = TaskInfo.newBuilder()
		            .setName("Task " + taskId.getValue())
		            .setTaskId(taskId)
		            .setSlaveId(offer.getSlaveId())
		            .addResources(Resource.newBuilder()
		                          .setName("cpus")
		                          .setType(Value.Type.SCALAR)
		                          .setScalar(Value.Scalar.newBuilder()
		                                     .setValue(1)
		                                     .build())
		                          .build())
		            .addResources(Resource.newBuilder()
		                          .setName("mem")
		                          .setType(Value.Type.SCALAR)
		                          .setScalar(Value.Scalar.newBuilder()
		                                     .setValue(25000)
		                                     .build())
		                          .build())
		            .setExecutor(executor)
		            .build();
		          tasks.add(task);
		          
		          
		        nodetaskindex.put(offerfromhostname,1);
		        Filters filters = Filters.newBuilder().setRefuseSeconds(3).build();
		        driver.launchTasks(offer.getId(), tasks, filters);	

		        launchedTasks++;
		        tasks=null;

		        }

	    }
	    
	    
	    String choosemultipletask(ArrayList<ArrayList<String>> tasklist, int taskindex, int tasknumber)
	    {
	    	String taskstring;
	    	
	        String tempstringfora = " ";
	        String tempstringforb = " ";
	        
	        for(int i=0;i<tasknumber;i++)
	        {
	        	String temp;
	        	if((taskindex+i)<tasklist.size())
	        	{
	        		temp = tasklist.get(taskindex+i).get(0) + " ";
	        	tempstringfora = tempstringfora + temp;
	        		
	        	}
	        	
	        }
	        
	        
	        for(int i=0;i<tasknumber;i++)
	        {
	        	String temp;
	        	if((taskindex+i)<tasklist.size())
	        	{
	        	temp = tasklist.get(taskindex+i).get(1) + " ";
	        	tempstringforb = tempstringforb + temp;
	        	}
	        	
	        }
	    		
	    	taskstring=	tempstringfora + tempstringforb;
	    	   	
	    	return taskstring;
	    }
	    
	      

	    @Override
	    public void resourceOffers(SchedulerDriver driver,
	                               List<Offer> offers) {
	    	
	    	if (strategymode.equals("dynamic")){

	    		for (Offer offer : offers)
	    			smartchoosedynamic(offer,driver,alltasks,workernodeandtaskconnectionlist,nodetaskindex);

	    	}else if(strategymode.equals("datamining")){

	    		for (Offer offer : offers)
	    			smartchoosedatamining(offer,driver,nodetaskindex);

	    	}else{
	    		
	    		for (Offer offer : offers) 
	    		    smartchoosestatic(offer,driver,alltaskmap,workernodeandtaskconnectionforstatic,nodetaskindex);
	    	}
	    }

	    @Override
	    public void offerRescinded(SchedulerDriver driver, OfferID offerId) {}

	    @Override
	    public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
	      System.out.println("Status update: task " + status.getTaskId().getValue() +
	                         " is in state " + status.getState());
	      if (status.getState() == TaskState.TASK_FINISHED) {
	        finishedTasks++;
	        System.out.println("Finished tasks: " + finishedTasks);
	        if (finishedTasks == totalTasks) {
	          driver.stop();
	        }
	      }
	    }

	    @Override
	    public void frameworkMessage(SchedulerDriver driver,
	                                 ExecutorID executorId,
	                                 SlaveID slaveId,
	                                 byte[] data) {}

	    @Override
	    public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {}

	    @Override
	    public void executorLost(SchedulerDriver driver,
	                             ExecutorID executorId,
	                             SlaveID slaveId,
	                             int status) {}

	    public void error(SchedulerDriver driver, String message) {
	      System.out.println("Error: " + message);
	    }

	    private final ExecutorInfo executor;
	    private int totalTasks;
	    private int launchedTasks = 0;
	    private int finishedTasks = 0;

	    HashMap<String, ArrayList<ArrayList<String>>> workernodeandtaskconnectionlist;	    
	    HashMap<String, List<List<String>>> workernodeandtaskconnectionforstatic;
    	ArrayList<ArrayList<ArrayList<String>>> taskslistpernodewithorder;
    	ArrayList<ArrayList<String>> alltasks;
    	ArrayList<HashMap<ArrayList<String>,Integer>> allpossibletaskspernodewithorder;
	    HashMap<Integer,List<List<String>>> alltaskmap;
    	HashMap<String, Integer> nodetaskindex;

	  }


		public void readsystemproperties(){

//		  	InputStream inputStream = JobTracker.class.getClassLoader().getResourceAsStream("ipConfig.properties");   
		    p = new Properties(); 
		    try{
		    	FileInputStream inputStream = new FileInputStream("/Users/Ivan/Documents/ipConfig.properties");
		    	p.load(inputStream);
		    }catch (IOException e1){
		    	e1.printStackTrace();
		    }
		}



		public void initialproperties(){
//			InputStream inputStream = JobTracker.class.getClassLoader().getResourceAsStream("ipConfig.properties");
		    p = new Properties(); 
		    try{
		    	FileInputStream inputStream = new FileInputStream("/Users/Ivan/Documents/ipConfig.properties");
		    	p.load(inputStream);
		    }catch (IOException e1){
		    	e1.printStackTrace();
		    }

		    allfilename=p.getProperty("allfilename");
	        datapernode=p.getProperty("datapernode");
	        inputdatalocation=p.getProperty("inputdatalocation");
	        statictaskpernode=p.getProperty("statictaskspernode");
	        numberofworkernodes=Integer.parseInt(p.getProperty("numberofworkernodes"));
	        strategymode=p.getProperty("strategymode");
	        dataminingpath1=p.getProperty("dataminingpath1");

            workernodelist=new ArrayList<String>();
	        for(int i=1;i<numberofworkernodes+1;i++)
	        	workernodelist.add(p.getProperty(i+""));

		}
			
        static String allfilename;
        static String datapernode;
        static String strategymode;
        static String statictaskpernode;
        static String inputdatalocation;
        static String dataminingpath1;      
	    static int numberofworkernodes;
	    static ArrayList<String> workernodelist;
	    Properties p;
	    
	    
	    String getallfilename(){
	    	return allfilename;
	    }
	    
	    String getdatapernode(){
	    	return datapernode;
	    }
	    
	    String getstrategymode(){
	    	return strategymode;
	    }
	    	    
	    int getnumberofworkernodes(){
	    	return numberofworkernodes;
	    }

	
	 private static void usage() {
		    String name = JobTracker.class.getName();
		    System.err.println("Usage: " + name + " master <tasks>");
		  }


	  public static void main(String[] args) throws Exception {
	    if (args.length < 1 || args.length > 2) {
	    	usage();
	    	System.exit(1);
	    }
	    
	    JobTracker jobtrackerins = new JobTracker();
	    jobtrackerins.initialproperties();
	    

	    int totaltasknumbers;
	    int tatalfilenumbers;
	    List<String> allfileforname = new ArrayList<String>();
	    


//	    allfileforname=SystemRun.Readfilename(allfilename);
//	    tatalfilenumbers=allfileforname.size();
//	    totaltasknumbers=tatalfilenumbers*(tatalfilenumbers-1)/2;   
	    totaltasknumbers=3;  

	    String uri = new File("./test-executor").getCanonicalPath();
    
	    ExecutorInfo executor = ExecutorInfo.newBuilder()
	      .setExecutorId(ExecutorID.newBuilder().setValue("Computation").build())
	      .setCommand(CommandInfo.newBuilder().setValue(uri).build())
	      .build();

	    FrameworkInfo framework = FrameworkInfo.newBuilder()
	        .setUser("") // Have Mesos fill in the current user.
	        .setName("Test Framework (Data Mining)")
	        .build();

	    MesosSchedulerDriver driver =  new MesosSchedulerDriver(
	        new JobScheduler(executor, totaltasknumbers),
	        framework,
	        args[0]);
	    	    
	    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
	  }
	
}	
