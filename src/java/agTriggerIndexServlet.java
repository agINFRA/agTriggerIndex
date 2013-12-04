/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import com.agroknow.indexer.App;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Collection;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.elasticsearch.client.Client;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;


import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.ClientResponse;
import java.io.BufferedInputStream;
import javax.ws.rs.core.MediaType;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.io.FileUtils;
/**
 *
 * @author nimas
 */
public class agTriggerIndexServlet extends HttpServlet {
    private String errorDescription;
    
    
    /*
     * Processes requests for both HTTP
     * <code>GET</code> and
     * <code>POST</code> methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
              
        //Params /home/setup/ 83.212.96.169 ds/ ds/archives/ akif ds/AKIF/ ds/runtime/ 1000                    
                String dspath = null;
		String localCouchdbProxy = null; // this is the IP for the server where the php script runs		
                String downloadDirectory= null; // e.g. /ds/
                String archiveDirectory=null;
		
                String fileFormat=null;
                String rootDirectory=null; //e.g/ /ds/AKIF
                String runtimeDirectory=null;
                String bulkSize =null;
                
        Map<String,String[]> params = request.getParameterMap() ;
        
       
        if ( params.containsKey( "CouchdbProxy" ) )
    	{
    		try
    		{  
                        dspath = "";                    
                        localCouchdbProxy=request.getParameter("CouchdbProxy") ;
                        downloadDirectory = "ds/";   
                        archiveDirectory = "ds/archives/";   
                        fileFormat = "akif";   
                        rootDirectory = "ds/AKIF/";   
                        runtimeDirectory = "ds/runtime/";           
                        bulkSize    = "1000";     
                                
                        buildIndex(dspath,localCouchdbProxy,downloadDirectory,archiveDirectory,fileFormat,rootDirectory,runtimeDirectory,bulkSize);
    		}
    		catch ( Exception e )
			{
				System.err.println( "Cannot access to" + request.getParameter("CouchdbProxy") + ": "  + e.getMessage() ) ;
			}
    	}
        
        response.setContentType("text/html;charset=UTF-8");
        PrintWriter out = response.getWriter();
        try {
            /* TODO output your page here. You may use following sample code. */
            out.println( "The elastic search index at the VM with IP-" +localCouchdbProxy+"-has now been updated." ) ;
        } finally {            
            out.close();
        }
    }
    
    
  
    private void buildIndex(String dspath,String localCouchdbProxy,String downloadDirectory,String archiveDirectory,String fileFormat, String rootDirectory,String runtimeDirectory,String bulkSize) throws Exception{
        
       /*
		//PARAMS
		//TODO: HAVE A CHECK ABOUT ARG NUMS
        
		String dspath = args[0];
		String localCouchdbProxy = args[1];		
                String downloadDirectory=args[2]; // e.g. /ds/
                String archiveDirectory=args[3];
		
                String fileFormat=args[4];
                String rootDirectory=args[5]; //e.g/ /ds/AKIF
                String runtimeDirectory=args[6];
                String bulkSize =args[7];
         */                   
        //      e.g.  home 83.212.96.169 ds/ ds/archives/ akif ds/AKIF/ ds/runtime/ 1000
                
			
		//Fetch and download IPB metadata sets.
		//CouchDB via PHP local proxy
		//http://agro.ipb.ac.rs/agcouchdb/_design/datasets/_view/list?limit=10
		//http://localhost/ag_couch_proxy/proxy-IPB-datasets.php
		try{
			System.out.println("Connecting IPB CouchDB...");                      
                        
			String url = "http://"+localCouchdbProxy+"/ag_couch_proxy/proxy-IPB-datasets.php?dspath="+dspath;
                        WebResource webResource = com.sun.jersey.api.client.Client.create().resource(url);			//System.out.println(url);
			ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON,MediaType.TEXT_HTML,MediaType.WILDCARD).get(ClientResponse.class);
			if (response.getStatus() != 200) {
			   throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
			}
			
			
			//String response_str = response.getEntity(String.class);	
			  String response_str = getStringFromInputStream(response.getEntityInputStream());
			//System.out.println(response_str);	//debug
			
			System.out.println("Finished IPB call:"+response_str);
			
			if(response_str.equals("")){
				errorDescription = "There are no new datasets available to download.";
			}																		           										                
                     }catch (Exception e) {
				e.printStackTrace();
				errorDescription = e.getMessage();
				System.out.println("    Some error:: ");
			}
                
                //foreach dataset.tar.gz **
		//Iterate 
		File root = new File(downloadDirectory);
		Collection files = FileUtils.listFiles(root, null, false);
		
		
		System.out.println("Iterating all downloaded datasets tgz files...");
		int dsCount = 0;
		
		for (Iterator iterator = files.iterator(); iterator.hasNext();) {
			File dsFile = (File) iterator.next();
			String inputDataset = dsFile.getAbsolutePath();
			
			dsCount = dsCount + 1;
			System.out.println("  Processing "+dsCount+":"+inputDataset);	
											
			//Uncompress the dataset and iterate throughout the files
			try {
				FileInputStream fin = new FileInputStream(inputDataset);
				BufferedInputStream in = new BufferedInputStream(fin);
				FileOutputStream out = new FileOutputStream(archiveDirectory +"archive.tar");
				GzipCompressorInputStream gzIn;		
				gzIn = new GzipCompressorInputStream(in);		
				final byte[] buffer = new byte[1024];
				int n = 0;
				while (-1 != (n = gzIn.read(buffer))) {
				    out.write(buffer, 0, n);
				}
				out.close();
				gzIn.close();
				
				//read the tar
				File input = new File(archiveDirectory+"/archive.tar"); //getFile("ds/archive.tar");			
		        InputStream is = new FileInputStream(input);
		        ArchiveInputStream in1 = new ArchiveStreamFactory().createArchiveInputStream("tar", is);
		        TarArchiveEntry entry = (TarArchiveEntry)in1.getNextEntry();
		        
		        while (entry != null) {// create a file with the same name as the tarEntry
		            File destPath = new File(rootDirectory + entry.getName());
		            if (entry.isDirectory()) {
		                destPath.mkdirs();
		            } else {
		                destPath.createNewFile();
		                OutputStream out1 = new FileOutputStream(destPath);
		                IOUtils.copy(in1, out1);
		                out1.close();
		            }
		            entry = (TarArchiveEntry)in1.getNextEntry();
		        }
		        
		        in1.close();
			} catch (Exception e) {
				e.printStackTrace();
				errorDescription = e.getMessage();
				System.out.println("*Error extracting");
			}
                
               
 
    // end method
    }
              App Elastic = new App();
              Elastic.main(new String[]{"--bulk-size",bulkSize,"--root-directory",rootDirectory,"--runtime-directory",runtimeDirectory,"--file-format",fileFormat});                             
    }
    
    
    
    // convert InputStream to String
	private String getStringFromInputStream(InputStream is) {
 
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
 
		String line;
		try {
 
			br = new BufferedReader(new InputStreamReader(is));
			while ((line = br.readLine()) != null) {
				sb.append(line+"\n");
			}
 
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
 
		return sb.toString();
 
	}

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP
     * <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Handles the HTTP
     * <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>
}
