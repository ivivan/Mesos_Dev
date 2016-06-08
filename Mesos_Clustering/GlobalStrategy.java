import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class GlobalStrategy {
	
	
	static HashMap<Integer,String> ReadTaskmap(String path)
	{
//		File inputfile = new File(path);
		
		
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
	
	
	HashMap<Integer,ArrayList<ArrayList<String>>> Processdatastringtotask(HashMap<Integer,String> datastringpernode,List<String> FileList)
	{
		HashMap<Integer,ArrayList<ArrayList<String>>> allpossibletasks= new HashMap<Integer,ArrayList<ArrayList<String>>>();
		
		
		
		Iterator<Integer> iterator = datastringpernode.keySet().iterator(); 
        
        while(iterator.hasNext()) { 	        		        	
        	
        	ArrayList<String> stringtolist = new ArrayList<String>();
        	
        	ArrayList<ArrayList<String>> allpossibletaskspernode = new ArrayList<ArrayList<String>>();
        	
        	Integer key = iterator.next();
        
		    String[] strarray=datastringpernode.get(key).split(" ");   
		    for (int i = 0; i < strarray.length; i++)   
		        stringtolist.add(FileList.get(Integer.parseInt(strarray[i]))); 
		    
		    for(int i=0;i<stringtolist.size();i++)
		    	for(int j=i+1;j<stringtolist.size();j++)
		    	{
		    		ArrayList<String> singletask = new ArrayList<String>();
		    		singletask.add(stringtolist.get(i));
		    		singletask.add(stringtolist.get(j));
		    		allpossibletaskspernode.add(singletask);
		    	}
		    
		    allpossibletasks.put(key, allpossibletaskspernode);
		   		
        }
				
		return allpossibletasks;
	}
	
	
	static List<String> Readfilename(String path)
	{
	
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
	
	
	
	
	
	HashMap<Integer,ArrayList<ArrayList<String>>> ReadDataperNode(String datapernodepath, String allfilename)
	{
		
		HashMap<Integer,ArrayList<ArrayList<String>>> allpossibletasks = new HashMap<Integer,ArrayList<ArrayList<String>>>();
		
		List<String> allfiles = new ArrayList<String>();
		
		HashMap<Integer,String> datapernode = new HashMap<Integer,String>();
		
		
		
		datapernode=ReadTaskmap(datapernodepath);
		
		allfiles=Readfilename(allfilename);
		
		allpossibletasks=Processdatastringtotask(datapernode,allfiles);
		
		
		
		return allpossibletasks;
	}
	
	
	      ArrayList<ArrayList<String>> GenerateAlltasks(List<String> filename)
	      {
	    	  ArrayList<ArrayList<String>> Alltasks = new ArrayList<ArrayList<String>>();
	    	  
	    	  
	    	  for(int i=0;i<filename.size();i++)
	    		  for(int j=i+1;j<filename.size();j++)
	    		  {
	    			  ArrayList<String> singletask = new ArrayList<String>();
	    			  singletask.add(filename.get(i));
	    			  singletask.add(filename.get(j));
	    			  Alltasks.add(singletask);
	    			  
	    		  }
	    			  
	    	  
	    	  return Alltasks;
	    	
	      }
	      
	      
	      
	      ArrayList<ArrayList<String>> GenerateAlltaskslist(String path)
	      {
	    	  
	    	  ArrayList<ArrayList<String>> Alltasks = new ArrayList<ArrayList<String>>();
	    	  List<String> filename = new ArrayList<String>();
	    	  
	    	  filename = Readfilename(path);
	    	  Alltasks = GenerateAlltasks(filename);
	    	  
	    	  
	    	  return Alltasks;
	    	  
	    	  
	      }
	      
	      
	      
	      
	
	
	ArrayList<HashMap<ArrayList<String>,Integer>> taskswithorderpernode (ArrayList<ArrayList<String>> alltasks,HashMap<Integer,ArrayList<ArrayList<String>>> taskspernode)
	{
	//	HashMap<ArrayList<String>,Integer> tasknumber = new HashMap<ArrayList<String>,Integer>();
		
		ArrayList<Integer> taskrelatednubmer = new ArrayList<Integer>();
		
	//	ArrayList<ArrayList<String>> tasksforeachnode = new ArrayList<ArrayList<String>>();
		ArrayList<HashMap<ArrayList<String>,Integer>> alltaskswithorderpernode = new ArrayList<HashMap<ArrayList<String>,Integer>>();
		
		
		
		for(int i=0;i<alltasks.size();i++)
			taskrelatednubmer.add(0);
		
//		System.out.println(taskrelatednubmer.size());
		
		Iterator<Integer> iterator = taskspernode.keySet().iterator(); 
		
		while(iterator.hasNext())
		{
			int index=0;
			Integer key = iterator.next();
			
			for(int i=0;i<taskspernode.get(key).size();i++)
			{
				
//				System.out.println(taskspernode.get(key).get(i));
				index=alltasks.indexOf(taskspernode.get(key).get(i));
//				System.out.println(index);
				
				taskrelatednubmer.set(index, taskrelatednubmer.get(index)+1);
			}
			
		}
		
		for(int i=0;i<taskspernode.size();i++)
		{
			
			HashMap<ArrayList<String>,Integer> taskswithnumberpernode = new HashMap<ArrayList<String>,Integer>();
			
			ArrayList<ArrayList<String>> tasklistforeachnode = new ArrayList<ArrayList<String>>();
			
			
			tasklistforeachnode=taskspernode.get(i);
			
			for(int j=0;j<tasklistforeachnode.size();j++)
			{
				taskswithnumberpernode.put(tasklistforeachnode.get(j),taskrelatednubmer.get(alltasks.indexOf(tasklistforeachnode.get(j))));
				
				
			}
			
			
		
			alltaskswithorderpernode.add(mapSortInteger(taskswithnumberpernode));
			
		}
		
		return alltaskswithorderpernode;
		
	}	
	
	
	
	
	public static HashMap<ArrayList<String> , Integer> mapSortInteger(Map<ArrayList<String> , Integer> map){
		
		LinkedHashMap<ArrayList<String> , Integer> orderedmap = new LinkedHashMap<ArrayList<String> , Integer>();
		List<Map.Entry<ArrayList<String>, Integer>> listData = new ArrayList<Map.Entry<ArrayList<String>, Integer>>(map.entrySet());

//		System.out.println(listData);
		
		Collections.sort(listData, new Comparator<Map.Entry<ArrayList<String>, Integer>>(){
		public int compare(Map.Entry<ArrayList<String>,Integer > o1, Map.Entry<ArrayList<String>, Integer> o2){
//		return (o2.getValue() - o1.getValue());
		return (o1.getValue() - o2.getValue());
		}
		}
		);
		
		Iterator<Map.Entry<ArrayList<String>, Integer>> iter = listData.iterator();  
        Map.Entry<ArrayList<String>, Integer> tmpEntry = null;  
        while (iter.hasNext()) {  
            tmpEntry = iter.next();  
            orderedmap.put(tmpEntry.getKey(), tmpEntry.getValue());  
        }  
        return orderedmap;
		
		
		
		

//		System.out.println(listData);
		}
	
	
	

	
	
	
	ArrayList<HashMap<ArrayList<String>,Integer>> getfinallist(String datapernodepath,String filenamepath)
	{
		List<String> allfiles = new ArrayList<String>();
		allfiles = Readfilename(filenamepath);
		
		
		
		ArrayList<ArrayList<String>> alltasks = new ArrayList<ArrayList<String>>();
		
		alltasks = GenerateAlltasks(allfiles);
//		System.out.println(alltasks.size());
//		System.out.println(alltasks.get(5).get(0));
//		System.out.println(alltasks.get(5).get(1));
		
		HashMap<Integer,ArrayList<ArrayList<String>>> allpossibletaskspernode = new HashMap<Integer,ArrayList<ArrayList<String>>>();
		
		allpossibletaskspernode=ReadDataperNode(datapernodepath, filenamepath);
		
//		System.out.println(allpossibletaskspernode.get(0).size());
//		System.out.println(allpossibletaskspernode.get(1).size());
//		System.out.println(allpossibletaskspernode.get(2).size());
//		System.out.println(allpossibletaskspernode.get(3).size());
		
		ArrayList<HashMap<ArrayList<String>,Integer>> finallist = new ArrayList<HashMap<ArrayList<String>,Integer>>();
		
		finallist = taskswithorderpernode(alltasks,allpossibletaskspernode);
		
		return finallist;
		
	}
	
	
	
	ArrayList<ArrayList<ArrayList<String>>> getkeyforeachmap(ArrayList<HashMap<ArrayList<String>,Integer>> finallists)
	{
		
		ArrayList<ArrayList<ArrayList<String>>> taskslistwithorder = new ArrayList<ArrayList<ArrayList<String>>>();
		
		
		for(int i=0;i<finallists.size();i++)
		{
			
			HashMap<ArrayList<String>,Integer> tasksmapwithorder = new HashMap<ArrayList<String>,Integer>();
			ArrayList<ArrayList<String>> taskspernodewithorder = new ArrayList<ArrayList<String>>();
			
			tasksmapwithorder=finallists.get(i);
			
			Iterator<ArrayList<String>> it = tasksmapwithorder.keySet().iterator();  
			
			while (it.hasNext()) {  
				
				ArrayList<String> key = it.next();  
				taskspernodewithorder.add(key);  
		       }  
			
			taskslistwithorder.add(taskspernodewithorder);
			
		}
		
		
		return taskslistwithorder;
		
			
		
	}
	
	
}
