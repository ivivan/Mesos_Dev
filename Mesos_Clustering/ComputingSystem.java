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


public class ComputingSystem<INDEX,CRUDEDATA,INTERMEDIATEDATA,RESULTDATA>{

	Map<INDEX,CRUDEDATA> Reader() throws IOException{
		return null;                        
	}
    
    INTERMEDIATEDATA Preprocess(CRUDEDATA crudedata){
    	return null;
    }

    RESULTDATA Compare(INTERMEDIATEDATA intermediatedataa,INTERMEDIATEDATA intermediatedatab){
    	return null;
    }

    void Writer(Matrix<INDEX,RESULTDATA> matrix, String taskvalue){
   
    }


    void Run() throws IOException{

    	Map<INDEX,CRUDEDATA> crudepairs;
        Map<INDEX,INTERMEDIATEDATA> intermediatepairs;
 		Map<List<INDEX>,RESULTDATA> resultpairs;
    	RESULTDATA resultdata;
    	List<INDEX> value;

	 /*
     * Running the Reader method providing by Users
     */
	    crudepairs = Reader();

	    Iterator<Entry<INDEX, CRUDEDATA>> crudeiterator = crudepairs.entrySet().iterator();
        intermediatepairs = new HashMap<INDEX,INTERMEDIATEDATA>();
            
        while(crudeiterator.hasNext()){

            Entry<INDEX, CRUDEDATA> entry = crudeiterator.next(); 
            intermediatepairs.put(entry.getKey(),Preprocess(entry.getValue()));	        
        }

        crudepairs=null;

    /*
     * Running Compare method providing by Users;
     */
        Iterator<Entry<INDEX,INTERMEDIATEDATA>> intermediateiterator = intermediatepairs.entrySet().iterator();
        resultpairs = new HashMap<List<INDEX>,RESULTDATA>();
        value = new ArrayList<INDEX>();

        while(intermediateiterator.hasNext()){

            Entry<INDEX, INTERMEDIATEDATA> entry = intermediateiterator.next(); 
            value.add(entry.getKey());   
        }

        resultdata = Compare(intermediatepairs.get(value.get(0)),intermediatepairs.get(value.get(1)));
        		
  //    resultdata = Compare(intermediatepairs.get(indexname),intermediatepairs.get(indexname+"1"));
        		      		
        System.out.println(resultdata);
        intermediatepairs=null;
    }
    
}
