import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.FileChannel;




/*
 * This Class provide by the System, 
 */

public class SystemRun {
	
	  static ComputingSystem<?, ?, ?, ?> Systemini(String classname) {
		  		  
//		  projectclassname = "Cvtreesystem";
		  ComputingSystem<?, ?, ?, ?> realproject = null;
	    	
		try {
				
				Class<?> Project = Class.forName(classname);	    	
	        	Constructor<?> localConstructor = Project.getConstructor(new Class[0]);
				realproject = (ComputingSystem<?, ?, ?, ?>) localConstructor.newInstance(new Object[0]);
				
			} catch (NoSuchMethodException | SecurityException | ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    			    
		  return realproject;		  
	  }
	
	
	
	
	public static List<File> GetaAllInputs(String inputdir){
		
		 File inputfiles = new File(inputdir);
//	     String[] filename = inputfiles.list();
	     File[] files= inputfiles.listFiles();
		 
	     List<File> allinputfiles = java.util.Arrays.asList(files);;
	     Collections.sort(allinputfiles);
	  
		 return allinputfiles;
	}
	
	
	  public static <T> List<List<T>> splitList(List<T> list, int pageSize) {
	        
	        int listSize = list.size();                                                          
	        int page = (listSize + (pageSize-1))/ pageSize;                     
	        
	        List<List<T>> listArray = new ArrayList<List<T>>();         
	        for(int i=0;i<page;i++) {                                                  
	            List<T> subList = new ArrayList<T>();                               
	            for(int j=0;j<listSize;j++) {                                                 
	                int pageIndex = ( (j + 1) + (pageSize-1) ) / pageSize;   
	                if(pageIndex == (i + 1)) {                                               
	                    subList.add(list.get(j));                                               
	                }
	                
	                if( (j + 1) == ((i + 1) * pageSize) ) {                               
	                    break;
	                }
	            }
	            listArray.add(subList);                                                         
	        }
	        return listArray;
	    }
	
	  
		static HashMap<Integer,String> ReadTaskmap(String path){
//			File inputfile = new File(path);
			
			
			FileReader reader;
			HashMap<Integer,String> taskmap = new HashMap<Integer,String>();
			
			
			try {
				int i=0;
				reader = new FileReader(path);
				BufferedReader br = new BufferedReader(reader);
			    String s = null;
			    while((s = br.readLine()) != null) {
			    	
			    	taskmap.put(i, s);
			    	i++;
			    	
			    	
			  }
				
			 br.close();
			 reader.close();
			
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return taskmap;
			
		}
	  
		
		

		  static HashMap<Integer,List<List<File>>> uncodetaskmapimproveforfile(HashMap<Integer,String> taskmap,List<File> FileList ){
				
				HashMap<Integer,List<List<File>>> uncodedtaskmap = new HashMap<Integer,List<List<File>>>();

				Iterator<Integer> iterator = taskmap.keySet().iterator(); 
		               
		        while(iterator.hasNext()) { 
		        	
		        	
		        	List<File> stringtolist = new ArrayList<File>();
		        	List<List<File>> splitlist = new ArrayList<List<File>>();
		        	
		        	Integer key = iterator.next();
		        
				    String[] strarray=taskmap.get(key).split(" ");   
				    for (int i = 0; i < strarray.length; i++)   
				        stringtolist.add(FileList.get(Integer.parseInt(strarray[i]))); 
				    
				    splitlist=splitList(stringtolist, 2);
				    uncodedtaskmap.put(key,splitlist);
				
			}
		        
			return uncodedtaskmap;
			}
	  

	  
	
	public static List<Object> GenerateTasks(List<File> lista, List<File> listb){
		List<Object> taskinput = new ArrayList<Object>();
		int lengtha,lengthb;
		lengtha=lista.size();
		lengthb=listb.size();
		
		for(int i=0;i<lengtha;i++)
			taskinput.add(lista.get(i));
		for(int j=0;j<lengthb;j++)
			taskinput.add(listb.get(j));
		taskinput.add(lengtha);
		taskinput.add(lengthb);
		
		return taskinput;
	}
	
	
	public <T> int calculatetotalTasks(List<List<T>> listArray){
		return listArray.size()+listArray.size()*(listArray.size()-1)/2;
	}
	
	public static HashMap<Integer,List<Object>> GenerateTasksTable(List<List<File>> listArray){
		int z=0;
		HashMap<Integer,List<Object>> TasksTable = new HashMap<Integer,List<Object>>();
		for(int i=0;i<listArray.size();i++)
			for(int j=i;j<listArray.size();j++)
			{
				TasksTable.put(z,GenerateTasks(listArray.get(i),listArray.get(j)));
				z++;
			}
		return TasksTable;
	}
	
	public static List<File> getlista(List<Object> tasklist){
		List<File> lista = new ArrayList<File>();
		int length,lengtha;
		length = tasklist.size();
		lengtha = (int) tasklist.get(length-2);
		for(int i=0;i<lengtha;i++)
			lista.add((File)tasklist.get(i));
		return lista;
	}
	
	public static List<File> getlistb(List<Object> tasklist){
		List<File> listb = new ArrayList<File>();
		int length,lengtha,lengthb;
		length = tasklist.size();
		lengtha = (int) tasklist.get(length-2);
		lengthb = (int) tasklist.get(length-1);
		for(int j=0;j<lengthb;j++)
			listb.add((File) tasklist.get(j+lengtha));
		return listb;
	}
	
	
	 public static void copyFile2(File srcFile, String destDir, String newFileName) {   
	        if (!srcFile.exists()) {  
	            System.out.println("");    
	        } else if (newFileName == null) {  
	            System.out.println("");   
	        } else {  
	            try {  
	                FileChannel fcin = new FileInputStream(srcFile).getChannel();  
	                FileChannel fcout = new FileOutputStream(new File(destDir,newFileName)).getChannel();  
	                fcin.transferTo(0, fcin.size(), fcout);  
	                fcin.close();  
	                fcout.close();  
	            } catch (FileNotFoundException e) {  
	                e.printStackTrace();  
	            } catch (IOException e) {  
	                e.printStackTrace();  
	            }  
	        }  
	    } 
	
	
	public static void getfilestolocal(List<File> listfilesa,List<File> listfilesb) throws IOException{
		String localdir = "/home/n8051020/Public/part/";
		String cache = "/home/n8051020/cache/";
		List<String> delfilelist = new ArrayList<String>();
		List<String> copyfilenamelist = new ArrayList<String>();
		
		File f = new File(cache);
		File[] files= f.listFiles();
		String[] filename = f.list();
		List<String> filenamelist = java.util.Arrays.asList(filename);
		
		for(int i=0;i<filenamelist.size();i++)
			copyfilenamelist.add(filenamelist.get(i));
		
		
	    if(filename.length==0)
	    {
	    	System.out.println("isEmpty!");
	    	for(int i = 0; i < listfilesa.size(); ++i)
		{
	    	copyFile2(listfilesa.get(i),cache,listfilesa.get(i).getName());
		}
	    	if (!(listfilesa.equals(listfilesb)))
	    	{
	    		for(int j = 0; j < listfilesb.size(); ++j)
	    			{
	    			copyFile2(listfilesb.get(j),cache,listfilesb.get(j).getName());
		            }
	    	}
	    }
		else
		{
			for(int i = 0; i < listfilesa.size(); ++i)
			{
				if (filenamelist.contains(listfilesa.get(i).getName()))
					delfilelist.add(listfilesa.get(i).getName());
				else
					copyFile2(listfilesa.get(i),cache,listfilesa.get(i).getName());
			}
			
			if (!(listfilesa.equals(listfilesb)))
			{
				for(int j = 0; j < listfilesb.size(); ++j)
				{
					if (filenamelist.contains(listfilesb.get(j).getName()))
						delfilelist.add(listfilesb.get(j).getName());
					else
						copyFile2(listfilesb.get(j),cache,listfilesb.get(j).getName());
			    }	
			}
			
				
		}
	    
	    copyfilenamelist.removeAll(delfilelist);
	    for(int i=0;i<copyfilenamelist.size();i++)
			{
				File file = new File(cache,copyfilenamelist.get(i));
				file.delete();
			}
	    
	}
	
	
	public static HashMap<Integer,List<Object>> preparetorun(String hdfsinputdir){
		List<File> inputfiles = new ArrayList<File>();
		List<List<File>> splitedlist = new ArrayList<List<File>>();
		HashMap<Integer,List<Object>> taskmap = new HashMap<Integer,List<Object>>();
		
		inputfiles=GetaAllInputs(hdfsinputdir);
		splitedlist = splitList(inputfiles,2);
		taskmap = GenerateTasksTable(splitedlist);
	
		return taskmap;
	}
	
	
	
	
	
	public static HashMap<Integer,List<List<File>>> preparetoruntwo(String hdfsinputdir){
		List<File> inputfiles = new ArrayList<File>();
		HashMap<Integer,String> taskmap = new HashMap<Integer,String>();
		HashMap<Integer,List<List<File>>> uncodedtaskmap = new HashMap<Integer,List<List<File>>>();
		
		inputfiles=GetaAllInputs(hdfsinputdir);
		taskmap=ReadTaskmap("/home/n8051020/Public/AllTasks.txt");
		uncodedtaskmap=uncodetaskmapimproveforfile(taskmap,inputfiles);
	
		return uncodedtaskmap;
	}
	
	
	
	public static HashMap<Integer,List<List<String>>> preparetorunthree(String allfilename,String alltasks){
		
		List<String> filename = new ArrayList<String>();
		HashMap<Integer,List<List<String>>> uncodedtaskmap = new HashMap<Integer,List<List<String>>>();
		HashMap<Integer,String> taskmap = new HashMap<Integer,String>();
		
		filename=Readfilename(allfilename);
//		taskmap=ReadTaskmap("/home/yifan/Public/AllTasks.txt");
		taskmap=ReadTaskmap(alltasks);
		
		uncodedtaskmap=uncodetaskmapimprove(taskmap,filename);
	
		return uncodedtaskmap;
	}
	
	
	 /*
	 * read all the name from a file, save as List<String>
	 * 
	 */
	static List<String> Readfilename(String path){
	
		FileReader reader;
		List<String> filename = new ArrayList<String>();
	
		try {
			
			reader = new FileReader(path);
			BufferedReader br = new BufferedReader(reader);
		    String s = null;
		    while((s = br.readLine()) != null) {	
		    	
		    	filename.add(s);	
		    	
		  }			
		 br.close();
		 reader.close();		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return filename;		
	}
	
	
	  static HashMap<Integer,List<List<String>>> uncodetaskmapimprove(HashMap<Integer,String> taskmap,List<String> FileList ){
			
			HashMap<Integer,List<List<String>>> uncodedtaskmap = new HashMap<Integer,List<List<String>>>();
			

			Iterator<Integer> iterator = taskmap.keySet().iterator(); 
	               
	        while(iterator.hasNext()) { 	        		        	
	        	
	        	List<String> stringtolist = new ArrayList<String>();
	        	List<List<String>> splitlist = new ArrayList<List<String>>();
	        	
	        	Integer key = iterator.next();
	        
			    String[] strarray=taskmap.get(key).split(" ");   
			    for (int i = 0; i < strarray.length; i++)   
			        stringtolist.add(FileList.get(Integer.parseInt(strarray[i]))); 
			    
			    splitlist=splitList(stringtolist, 2);	
			    uncodedtaskmap.put(key,splitlist);
			
		}       
		return uncodedtaskmap;
		}
	
	
	
	
//		static ComputingSystem<?, ?, ?, ?> theinstance;


	    public static void run(int lines) throws IOException {

			int numbersoftask;
			
			ComputingSystem<?, ?, ?, ?> theinstance;		
			theinstance=Systemini("Datamining");
			numbersoftask=lines;
		
		    for(int i =0;i<numbersoftask;i++) 
		    	theinstance.Run();
	    }
}
