package smartkv.datastore.workloads;



import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class WebService implements Runnable{
		ActivityEvent initNetwork, turnOffLinks, turnOffSwitches,addremoveTest; 
		RequestLogger log;
		
		WebService (RequestLogger log){
			this.log = log; 
		}
		
		@Override
		public void run() {
		/*	try {
				ServerSocket server = new ServerSocket(8000);
				int i =0;
				
				while (true){
					Socket clientSocket  = server.accept();
					InputStream sin;
					try {
						sin = clientSocket.getInputStream();
						DataInputStream in = new DataInputStream(clientSocket.getInputStream());
						String msg = in.readLine();
						System.out.println(msg);
						if (msg.equals("INIT_NETWORK")){
							initNetwork = log.addActivity(ActivityEvent.startNetwork());
						}else if (msg.equals("INIT_NETWORK_END")){
							log.endActivity(initNetwork);
						}else if (msg.equals("TURN_OFF_LINKS")){
							turnOffLinks = log.addActivity(ActivityEvent.removingLinks());
						}else if (msg.equals("TURN_OFF_LINKS_END")){
							log.endActivity(turnOffLinks); 
						}else if (msg.equals("TURN_OFF_SWITCHES")){
							turnOffSwitches = log.addActivity(ActivityEvent.newSwitchTurnOff()); 
						}else if (msg.equals("TURN_OFF_SWITCHES_END")){
							log.endActivity(turnOffSwitches);
							System.out.println("Sleep before saving"); 
							try {
								Thread.currentThread().sleep(5*  60 * 1000);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							finally{
								endAll(log); 
							}
						}else if (msg.equals("ADDING_REMOVE_TEST")){
							//addremoveTest = log.addActivity(ActivityEvent.); 
						}else if (msg.equals("ADDING_REMOVE_TEST_END")){
							 log.endActivity(addremoveTest);
						}
						else if (msg.equals("INIT_NETWORK_END")){
							log.endActivity(initNetwork);
							
						}
						
						clientSocket.close(); 
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			*/
			// TODO Auto-generated method stub
			
		}

		private void endAll(RequestLogger log) {
			System.out.println("Ending"); 
			log.end("timed"); 
			
		}
	}
	
