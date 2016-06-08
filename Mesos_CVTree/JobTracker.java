import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	      this.executor = executor;
	      this.totalTasks = totalTasks;
	      
	      
	      GlobalStrategy global = new GlobalStrategy();	      
	      
	      this.allpossibletaskspernodewithorder=global.getfinallist(this.datapernode, this.allfilename);	
	      
	      this.taskslistpernodewithorder=global.getkeyforeachmap(this.allpossibletaskspernodewithorder);
	 
	      
	      
	      this.tasklistforthreewithorder=this.taskslistpernodewithorder.get(0);
	      this.tasklistforfourwithorder=this.taskslistpernodewithorder.get(1);
	      this.tasklistforfivewithorder=this.taskslistpernodewithorder.get(2);
	      this.tasklistforsixwithorder=this.taskslistpernodewithorder.get(3);
	      
	      
	      this.alltasknumbersofthree=this.tasklistforthreewithorder.size();
	      this.alltasknumbersoffour=this.tasklistforfourwithorder.size();
	      this.alltasknumbersoffive=this.tasklistforfivewithorder.size();
	      this.alltasknumbersofsix=this.tasklistforsixwithorder.size();
	      
	      
	      
	      
//	      this.tasksforthreewithorder=allpossibletaskspernodewithorder.get(0);
//	      this.tasksforfourwithorder=allpossibletaskspernodewithorder.get(1);
//	      this.tasksforfivewithorder=allpossibletaskspernodewithorder.get(2);
//	      this.tasksforsixwithorder=allpossibletaskspernodewithorder.get(3);
	      this.alltasks=global.GenerateAlltaskslist(this.allfilename);
	
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
	    
	    
	    public void smartchoose(Offer offer,SchedulerDriver driver,ArrayList<ArrayList<String>> alltasks)
	    {
	    	
	    	String Stringvalue;
	    	String Disturbname;
	    	
	    	if(offer.getHostname().contains("6"))
	    	{
	    	   
		        if ((launchedTasks < totalTasks)&&(indexthree<alltasknumbersofthree)) {
		        	
		        	int temp=0;
		        	while(unlunchedforthree)
		        	{
		        		temp=alltasks.indexOf(tasklistforthreewithorder.get(indexthree));
//		        		if(alltasks.contains(tasklistforthreewithorder.get(indexthree)))
		        		if(!(temp<0))
		        		{
		        			
		        			 List<TaskInfo> tasks = new ArrayList<TaskInfo>();
		        			 
		        			 
		        			 Stringvalue=choosemultipletask(tasklistforthreewithorder, indexthree, 1);
		        			 
		        			 
		        			 
		 		        	
		 		    //    	 Stringvalue=tasklistforthreewithorder.get(indexthree).get(0)+" "+tasklistforthreewithorder.get(indexthree).get(1);
		 		        	
		 		        	
		 		        	
		 		          TaskID taskId = TaskID.newBuilder()
		 		        		  .setValue(Stringvalue).build();

		 		          System.out.println("Launching task " + taskId.getValue());		          	          
		 		         

		 		          TaskInfo task = TaskInfo.newBuilder()
		 		            .setName("Comparetask " + taskId.getValue())
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
		 		        
		 		        
		 		        
		 		        
//		 		        alltasks.remove(tasklistforthreewithorder.get(indexthree));
		 		        alltasks.remove(temp);
		 		        indexthree++;
		 		        launchedTasks++;
		 		        
		 		        
		 		        tasks=null;
		        			
		 		        unlunchedforthree=false;	
		        			
		        
		    
		        			
		        		}
		        		else
		        		{
		        			indexthree++;
                                                if(indexthree>=alltasknumbersofthree){
//indexthree=0;
                                                unlunchedforthree=false;

}
		        			
		        		}
		        		
		        		
		        	}
		        	
		        	
		     
		          
		          
		        }
		       
		      }
	  
	    	if(offer.getHostname().contains("8"))
	    	{
	    		if(!disturbforfour){




	    		





	    	    
		        if ((launchedTasks < totalTasks)&&(indexfour<alltasknumbersoffour)) {
		        	
		        	int temp=0;
		        	while(unlunchedforfour)
		        	{
		        		temp=alltasks.indexOf(tasklistforfourwithorder.get(indexfour));
//		        		if(alltasks.contains(tasklistforfourwithorder.get(indexfour)))
		        		if(!(temp<0))
		        		{
		        			
		        			 List<TaskInfo> tasks = new ArrayList<TaskInfo>();
		 		        	
Stringvalue=choosemultipletask(tasklistforfourwithorder, indexfour, 1);

//		 		        	 Stringvalue=tasklistforfourwithorder.get(indexfour).get(0)+" "+tasklistforfourwithorder.get(indexfour).get(1);
		 		        	
		 		        	
		 		        	
		 		          TaskID taskId = TaskID.newBuilder()
		 		        		  .setValue(Stringvalue).build();

		 		          System.out.println("Launching task " + taskId.getValue());		          	          
		 		         

		 		          TaskInfo task = TaskInfo.newBuilder()
		 		            .setName("Comparetask " + taskId.getValue())
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
		 		        
		 		        
		 		        
		 		        
//		 		        alltasks.remove(tasklistforfourwithorder.get(indexfour));
		 		        alltasks.remove(temp);
		 		        indexfour++;
		 		        launchedTasks++;
		 		        
		 		        
		 		        tasks=null;
		        			
		 		        unlunchedforfour=false;	
		        			
		        
		    
		        			
		        		}
		        		else
		        		{
		        			indexfour++;
                            if(indexfour>=alltasknumbersoffour){
                                unlunchedforfour=false;

                             }

                                                

		        			
		        		}
		        		
		        		
		        	}
		        	
		        	
		     
		          
		          
		        }

		    }else{

		    	List<TaskInfo> tasks = new ArrayList<TaskInfo>();	
		    	Disturbname="Disturb";	 		        			 		        	
		 		        	
		 		          TaskID taskId = TaskID.newBuilder()
		 		        		  .setValue(Disturbname).build();

		 		          System.out.println("Launching task " + taskId.getValue());		          	          
		 		         

		 		          TaskInfo task = TaskInfo.newBuilder()
		 		            .setName("DisturbingTask " + taskId.getValue())
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
		 		        
		 		        tasks=null;
		        			
		 		        disturbforfour=false;	





		    }






	    	}
	    	
	    	if(offer.getHostname().contains("9"))
	    	{
		        if ((launchedTasks < totalTasks)&&(indexfive<alltasknumbersoffive)) {
		        	
		        	int temp=0;
		        	while(unlunchedforfive)
		        	{
		        		
//		        		if(alltasks.contains(tasklistforfivewithorder.get(indexfive)))
		        		temp=alltasks.indexOf(tasklistforfivewithorder.get(indexfive));
		        		if(!(temp<0))
		        		{
		        			
		        			 List<TaskInfo> tasks = new ArrayList<TaskInfo>();

Stringvalue=choosemultipletask(tasklistforfivewithorder, indexfive, 1);
		 		        	
//		 		        	 Stringvalue=tasklistforfivewithorder.get(indexfive).get(0)+" "+tasklistforfivewithorder.get(indexfive).get(1);
		 		        	
		 		        	
		 		        	
		 		          TaskID taskId = TaskID.newBuilder()
		 		        		  .setValue(Stringvalue).build();

		 		          System.out.println("Launching task " + taskId.getValue());		          	          
		 		         

		 		          TaskInfo task = TaskInfo.newBuilder()
		 		            .setName("Comparetask " + taskId.getValue())
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
		 		        
		 		        
		 		        
		 		        
//		 		        alltasks.remove(tasklistforfivewithorder.get(indexfive));
		 		        alltasks.remove(temp);
		 		        indexfive++;
		 		        launchedTasks++;
		 		        
		 		        
		 		        tasks=null;
		        			
		 		        unlunchedforfive=false;	
		        			
		        
		    
		        			
		        		}
		        		else
		        		{
		        			indexfive++;
if(indexfive>=alltasknumbersoffive){
//indexfive=0;
                                                unlunchedforfive=false;

		        	}		
		        		}
		        		
		        		
		        	}
		        	
		        	
		     
		          
		          
		        }
	    	}
	    	
	    	if(offer.getHostname().contains("7"))
	    	{
	    		if(!disturbforsix){
		        if ((launchedTasks < totalTasks)&&(indexsix<alltasknumbersofsix)) {
		        	
		        	int temp=0;
		        	while(unlunchedforsix)
		        	{
		        		
//		        		if(alltasks.contains(tasklistforsixwithorder.get(indexsix)))
		        		temp=alltasks.indexOf(tasklistforsixwithorder.get(indexsix));
		        		if(!(temp<0))
		        		{
		        			
		        			 List<TaskInfo> tasks = new ArrayList<TaskInfo>();
		 		
Stringvalue=choosemultipletask(tasklistforsixwithorder, indexsix, 1);        	
//		 		        	 Stringvalue=tasklistforsixwithorder.get(indexsix).get(0)+" "+tasklistforsixwithorder.get(indexsix).get(1);
		 		        	
		 		        	
		 		        	
		 		          TaskID taskId = TaskID.newBuilder()
		 		        		  .setValue(Stringvalue).build();

		 		          System.out.println("Launching task " + taskId.getValue());		          	          
		 		         

		 		          TaskInfo task = TaskInfo.newBuilder()
		 		            .setName("Comparetask " + taskId.getValue())
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
		 		        
		 		        
		 		        
		 		        
//		 		        alltasks.remove(tasklistforsixwithorder.get(indexsix));
		 		        alltasks.remove(temp);
		 		        indexsix++;
		 		        launchedTasks++;
		 		        
		 		        
		 		        tasks=null;
		        			
		 		        unlunchedforsix=false;	
		        			
		        
		    
		        			
		        		}
		        		else
		        		{
		        			indexsix++;
                            if(indexsix>=alltasknumbersofsix){
                            unlunchedforsix=false;

		                    }      			
		        		}
		        		
		        		
		        	}
		        	
		        	
		     
		          
		          
		        }
		    }else{
		    	List<TaskInfo> tasks = new ArrayList<TaskInfo>();	
		    	Disturbname="Disturb";	 		        			 		        	
		 		        	
		 		          TaskID taskId = TaskID.newBuilder()
		 		        		  .setValue(Disturbname).build();

		 		          System.out.println("Launching task " + taskId.getValue());		          	          
		 		         

		 		          TaskInfo task = TaskInfo.newBuilder()
		 		            .setName("DisturbingTask " + taskId.getValue())
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
		 		        
		 		        tasks=null;
		        			
		 		        disturbforsix=false;



		    }

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
	      for (Offer offer : offers) {
	      	if(finishedTasks%10==0){
	    	  	disturbforfour=true;
	    	  	disturbforsix=true;
	    	  }
	    	  
	    	  smartchoose(offer,driver,alltasks);
	    	  
	  	    unlunchedforthree = true;
		    unlunchedforfour = true;
		    unlunchedforfive = true;
		    unlunchedforsix = true;
	    	
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
//	    HashMap<Integer,List<List<String>>> alltaskmap;
//	    List<List<String>> taskforthree;
//    	List<List<String>> taskforfour ;
//    	List<List<String>> taskforfive ;
//    	List<List<String>> taskforsix;
    	int tasksnumberforthree=0;
    	int tasksnumberforfour=0;
    	int tasksnumberforfive=0;
    	int tasksnumberforsix=0;
	    
	    Boolean unlunchedforthree = true;
	    Boolean unlunchedforfour = true;
	    Boolean unlunchedforfive = true;
	    Boolean unlunchedforsix = true;


	    Boolean disturbforfour = false;
	    Boolean disturbforsix = false;




	    
	    
    	ArrayList<ArrayList<ArrayList<String>>> taskslistpernodewithorder;
    	ArrayList<ArrayList<String>> alltasks;
    	ArrayList<HashMap<ArrayList<String>,Integer>> allpossibletaskspernodewithorder;
    	
    	ArrayList<ArrayList<String>> tasklistforthreewithorder;
    	ArrayList<ArrayList<String>> tasklistforfourwithorder;
    	ArrayList<ArrayList<String>> tasklistforfivewithorder;
    	ArrayList<ArrayList<String>> tasklistforsixwithorder;
    	
//    	HashMap<ArrayList<String>,Integer> tasksforthreewithorder;
//    	HashMap<ArrayList<String>,Integer> tasksforfourwithorder;
//    	HashMap<ArrayList<String>,Integer> tasksforfivewithorder;
//    	HashMap<ArrayList<String>,Integer> tasksforsixwithorder;
    	
    	int alltasknumbersofthree=0;
    	int alltasknumbersoffour=0;
    	int alltasknumbersoffive=0;
    	int alltasknumbersofsix=0;
    	
    	
	    
	    int indexthree=0;
    	int indexfour=0;
    	int indexfive=0;
    	int indexsix=0;
    	String allfilename = "/home/n8051020/Public/allfiles.txt";
 //       String alltasks = "/home/yifan/Public/AllTasks.txt";
        String datapernode="/home/n8051020/Public/datapernode.txt";
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
	    
	    int totaltasknumbers;
	    int tatalfilenumbers;
		String allfilename = "/home/n8051020/Public/allfiles.txt";
	    
	    

	    List<String> allfileforname = new ArrayList<String>();
	    allfileforname=SystemRun.Readfilename(allfilename);
	    tatalfilenumbers=allfileforname.size();
	    
	    allfileforname=null;
	    
	    totaltasknumbers=tatalfilenumbers*(tatalfilenumbers-1)/2;
	    

	    String uri = new File("./test-executor").getCanonicalPath();

	    
	    
	    ExecutorInfo executor = ExecutorInfo.newBuilder()
	      .setExecutorId(ExecutorID.newBuilder().setValue("PreprocessandCompare").build())
	      .setCommand(CommandInfo.newBuilder().setValue(uri).build())
	      .build();

	    FrameworkInfo framework = FrameworkInfo.newBuilder()
	        .setUser("") // Have Mesos fill in the current user.
	        .setName("Test Framework (CVTREE)")
	        .build();

	    MesosSchedulerDriver driver =  new MesosSchedulerDriver(
	        new JobScheduler(executor, totaltasknumbers),
	        framework,
	        args[0]);
	    
	   

	    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
	  }
	
}	
