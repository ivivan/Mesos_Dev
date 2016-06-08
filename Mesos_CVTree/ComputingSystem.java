import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


/*
 * System interface, users need to implement all the four methods
 */


public class ComputingSystem<INDEX,CRUDEDATA,INTERMEDIATEDATA,RESULTDATA> 
{
	Map<INDEX,CRUDEDATA> Reader()
	{
		return null;                        
	}
    
    INTERMEDIATEDATA Preprocess(CRUDEDATA crudedata)
    {
    	return null;
    }

    RESULTDATA Compare(INTERMEDIATEDATA intermediatedataa,INTERMEDIATEDATA intermediatedatab)
    {
    	return null;
    }

    void Writer(Matrix<INDEX,RESULTDATA> matrix, String taskvalue)
    {
   
    }
    
    void Run(List<File> lista, List<File> listb, String TaskID)
    {
    	
    	Map<INDEX,CRUDEDATA> crudepairs;
		Map<INDEX,INTERMEDIATEDATA> intermediatepairs;
		Map<List<INDEX>,RESULTDATA> resultpairs;
		RESULTDATA resultdata;
		List<INDEX> listindex;
		List<INDEX> listindextwo;
		Matrix<INDEX,RESULTDATA> resultmatrix;
		
		List<String> listaname = new ArrayList<String>();
		List<String> listbname = new ArrayList<String>();
		
		for(int i=0;i<lista.size();i++)
			listaname.add(lista.get(i).getName());
		
		for(int j=0;j<listb.size();j++)
			listbname.add(listb.get(j).getName());
		
//		try {
//			SystemRun.getfilestolocal(lista, listb);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
    	
    	 /*
         * Running the Reader method providing by Users
         */
		crudepairs = Reader();
		
		/*
		 * choose needed key value pair
		 */
		
		Iterator<Entry<INDEX, CRUDEDATA>> crudeiteratorfirst = crudepairs.entrySet().iterator();
		
		while(crudeiteratorfirst.hasNext())
        {
            Entry<INDEX, CRUDEDATA> entry = crudeiteratorfirst.next(); 
            INDEX key = entry.getKey();
            if(!(key.equals(listaname.get(0))||key.equals(listbname.get(0))))
            {
            	crudeiteratorfirst.remove();
            }
        
        }
		
		
		
		if(timer!=0)
		{			
			@SuppressWarnings("unchecked")
			HashMap<INDEX,INTERMEDIATEDATA> intertemp = (HashMap<INDEX, INTERMEDIATEDATA>) interresultinmem;
			
			
			Iterator<Entry<INDEX,INTERMEDIATEDATA>> intertempiterator = intertemp.entrySet().iterator();
			
			listindextwo =new ArrayList<INDEX>();
			
			while(intertempiterator.hasNext())
	        {
	            Entry<INDEX, INTERMEDIATEDATA> entry = intertempiterator.next(); 
	            listindextwo.add(entry.getKey());
	        
	        }
			
			Iterator<Entry<INDEX, CRUDEDATA>> crudeiterator = crudepairs.entrySet().iterator();
		    intermediatepairs = new HashMap<INDEX,INTERMEDIATEDATA>();
		            
		        while(crudeiterator.hasNext())
		        {
		            Entry<INDEX, CRUDEDATA> entry = crudeiterator.next(); 
		            if(listindextwo.contains(entry.getKey()))
		            {
		            	intermediatepairs.put(entry.getKey(),intertemp.get(entry.getKey()));
		            }
		            else
		            {
		            	intermediatepairs.put(entry.getKey(),Preprocess(entry.getValue()));
		            }
		            
		        
		        }
			
		
			
			
		}
		
		else
		{
			
			
			
	        Iterator<Entry<INDEX, CRUDEDATA>> crudeiterator = crudepairs.entrySet().iterator();
	        intermediatepairs = new HashMap<INDEX,INTERMEDIATEDATA>();
	            
	        while(crudeiterator.hasNext())
	        {
	            Entry<INDEX, CRUDEDATA> entry = crudeiterator.next(); 
	            intermediatepairs.put(entry.getKey(),Preprocess(entry.getValue()));
	        
	        }
			
		}
		
		
		
		
		crudepairs=null;
	        
	    interresultinmem=(HashMap<?, ?>) intermediatepairs;
		
	    timer=1;     
		
		

        /*
         * Running the Preprocess method providing by Users
         */
/*        Iterator<Entry<INDEX, CRUDEDATA>> crudeiterator = crudepairs.entrySet().iterator();
        intermediatepairs = new HashMap<INDEX,INTERMEDIATEDATA>();
            
        while(crudeiterator.hasNext())
        {
            Entry<INDEX, CRUDEDATA> entry = crudeiterator.next(); 
            intermediatepairs.put(entry.getKey(),Preprocess(entry.getValue()));
        
        }
        crudepairs=null;*/
        /*
         * Running Compare method providing by Users;
         */
        Iterator<Entry<INDEX,INTERMEDIATEDATA>> intermediateiterator = intermediatepairs.entrySet().iterator();
        listindex = new ArrayList<INDEX>();   
        resultpairs = new HashMap<List<INDEX>,RESULTDATA>();
        
        while(intermediateiterator.hasNext())
        {
            Entry<INDEX, INTERMEDIATEDATA> entry = intermediateiterator.next();
            listindex.add(entry.getKey());
        }
        
        for(int i=0;i<listindex.size();i++)
            for(int j=i+1;j<listindex.size();j++)
            {  	        	
        		
            	if (((listaname.contains(listindex.get(i)))&&(listbname.contains(listindex.get(j))))||((listbname.contains(listindex.get(i)))&&(listaname.contains(listindex.get(j)))))
            	{
            		List<INDEX> list = new ArrayList<INDEX>();
			        resultdata = Compare(intermediatepairs.get(listindex.get(i)),intermediatepairs.get(listindex.get(j)));
		        	list.add(listindex.get(i));
                    list.add(listindex.get(j));
                    resultpairs.put(list,resultdata);
            	}
            			
            	else ;		
            			
			}
  
        intermediatepairs=null;
        /*
         * Running Writer Method providing by Users;
         */
        resultmatrix = new Matrix<INDEX,RESULTDATA>(resultpairs,listindex);
        Writer(resultmatrix,TaskID);	
        resultmatrix=null;
    }
    
    
    static HashMap<?,?> interresultinmem;
    static int timer=0;  
    
    
}
