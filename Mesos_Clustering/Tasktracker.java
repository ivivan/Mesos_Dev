import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;




public class Tasktracker implements Executor{
	
	
	@Override
	  public void registered(ExecutorDriver driver,
	                         ExecutorInfo executorInfo,
	                         FrameworkInfo frameworkInfo,
	                         SlaveInfo slaveInfo) {
	    System.out.println("Registered executor on " + slaveInfo.getHostname());
	  }

	  @Override
	  public void reregistered(ExecutorDriver driver, SlaveInfo executorInfo) {}

	  @Override
	  public void disconnected(ExecutorDriver driver) {}

	  @Override
	  public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
		  	  
	    new Thread() { public void run() {
	      try {
	        TaskStatus status = TaskStatus.newBuilder()
	          .setTaskId(task.getTaskId())
	          .setState(TaskState.TASK_RUNNING).build();

	        driver.sendStatusUpdate(status);

            System.out.println("Running task " + task.getTaskId());
            
            String path1 = task.getTaskId().getValue();

String[] str=path1.split(" ");

//System.out.println(str[0]);_

	        List<String> lines = Files.readAllLines(Paths.get(str[0]), StandardCharsets.UTF_8);
	        int totalnumber=lines.size();
            
            SystemRun.run(totalnumber);    





/*	        String i = task.getTaskId().getValue();
            System.out.println("Running task " + task.getTaskId());
            SystemRun2.run(i);  */     

	        status = TaskStatus.newBuilder()
	          .setTaskId(task.getTaskId())
	          .setState(TaskState.TASK_FINISHED).build();

	        driver.sendStatusUpdate(status);
	      } catch (Exception e) {
	        e.printStackTrace();
	      }
	    }}.start();
	  }

	  @Override
	  public void killTask(ExecutorDriver driver, TaskID taskId) {}

	  @Override
	  public void frameworkMessage(ExecutorDriver driver, byte[] data) {}

	  @Override
	  public void shutdown(ExecutorDriver driver) {}

	  @Override
	  public void error(ExecutorDriver driver, String message) {}

	  public static void main(String[] args) throws Exception {
	    MesosExecutorDriver driver = new MesosExecutorDriver(new Tasktracker());
	    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
	  }

}
