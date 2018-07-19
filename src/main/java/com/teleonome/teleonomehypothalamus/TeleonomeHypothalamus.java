package com.teleonome.teleonomehypothalamus;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.Vector;
import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.teleonome.framework.TeleonomeConstants;
import com.teleonome.framework.denome.Identity;
import com.teleonome.framework.exception.InvalidDenomeException;
import com.teleonome.framework.hypothalamus.Hypothalamus;
import com.teleonome.framework.hypothalamus.PulseThread;
import com.teleonome.framework.network.NetworkUtilities;
import com.teleonome.framework.utils.Utils;

public class TeleonomeHypothalamus extends Hypothalamus{

	JSONObject organismViewStatusInfoJSONObject = new JSONObject();
	public final static Logger observerThreadLogger = Logger.getLogger(TeleonomeHypothalamus.class.getName() + "." + ObserverThread.class.getSimpleName());
	public final static Logger subscriberThreadLogger = Logger.getLogger(TeleonomeHypothalamus.class.getName() + "." + SubscriberThread.class.getSimpleName());
	public final static Logger selfSubscriberThreadLogger = Logger.getLogger(TeleonomeHypothalamus.class.getName() + "." + SubscriberThread.class.getSimpleName());
	public final static String BUILD_NUMBER="05/06/2018 13:36";
	PulseThread aPulseThread = new PulseThread(this);
	Hashtable teleonomeNamePulseIsLateIndex = new Hashtable();
	public long selfPublisherLastPulseTimeMillis=0;
	
	public TeleonomeHypothalamus(){

		logger.warn("Pacemaker build " + BUILD_NUMBER + " Process Number:" + processName);  

		try {
			FileUtils.writeStringToFile(new File("TeleonomeHypothalamusBuild.info"), BUILD_NUMBER);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			logger.warn(Utils.getStringException(e1));
		}

		ObserverThread o = new ObserverThread();
		o.start();


		aPulseThread.start();



	}


	class ObserverThread extends Thread{
		JmDNS mdnsServer;
		int subscriberThreadCounter=0;
		
		String thisTeleonomeName;
		String thisTeleonomAddress;
		public ObserverThread(){
			String bonjourServiceType = "_teleonome._tcp.local.";


			try {
				observerThreadLogger.info("about to create teleonome service");
				mdnsServer = JmDNS.create(getIpAddress());
				// Register a test service.
				ServiceInfo testService = ServiceInfo.create(bonjourServiceType, hostName, 6666, "Teleonome service");
				mdnsServer.registerService(testService);
				logger.info("created teleonome service");
				thisTeleonomeName = aDenomeManager.getDenomeName();
				
								
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.warn(Utils.getStringException(e));
			}

		}

		public void run(){
			observerThreadLogger.debug("Starting ObserverThread");
			String address="";
			String contents="";
			String teleonomName="";
			String teleonomAddress="";

			Iterator whoAmIKeys=null;
			String whoAmIAttributeName="";
			String whoAmIAttributeType="";
			
			SubscriberThread aSubscriberThread;	
			Hashtable presentTeleonoms;
			Hashtable notPresentTeleonoms;
			String status=null;
			String operationMode=null;
			String identity;
			
			do{
				try {
					 presentTeleonoms = aDiscoverTeleonoms.getPresentTeleonoms();
					 notPresentTeleonoms = aDiscoverTeleonoms.getNotPresentTeleonoms();
					observerThreadLogger.debug("there are notPresentTeleonoms=" + notPresentTeleonoms.size());
					observerThreadLogger.debug("teleonomesToReconnect=" + teleonomesToReconnect + " teleonomesToReconnect.size()=" + teleonomesToReconnect.size());

					
					
					for(Enumeration<String> en=notPresentTeleonoms.keys();en.hasMoreElements();){
						teleonomName = (String)en.nextElement();
						observerThreadLogger.debug("Not Found teleonome " + teleonomName);
						organismViewStatusInfoJSONObject.put(teleonomName,"faded");
					}
					publishToHeart(TeleonomeConstants.HEART_TOPIC_ORGANISM_STATUS, organismViewStatusInfoJSONObject.toString());

					for(Enumeration<String> en=subscriberList.keys();en.hasMoreElements();){
						teleonomAddress = (String)en.nextElement();
						subscriber=(Socket)subscriberList.get(teleonomAddress );
						if(subscriber!=null) {
							observerThreadLogger.debug("in observer thread teleonomAddress=" + teleonomAddress + " has a subscriber connected");

						}

					}

					//logger.debug("presentTeleonoms=" + presentTeleonoms.size());
					for(Enumeration<String> en=presentTeleonoms.keys();en.hasMoreElements();){

						teleonomName = (String)en.nextElement();
						if(!thisTeleonomeName.equals(teleonomName)) {
							teleonomAddress = (String)presentTeleonoms.get(teleonomName);					

							 status = TeleonomeConstants.TELEONOME_STATUS_DISCOVERED;
							 operationMode=TeleonomeConstants.TELEONOME_OPERATION_MODE_UNKNOWN;
							 identity = TeleonomeConstants.TELEONOME_IDENTITY_ORGANISM;
							aDenomeManager.registerTeleonome(teleonomName,status, operationMode, identity, teleonomName + ".local", teleonomAddress);
							subscriber = (Socket)subscriberList.get(teleonomAddress);
							observerThreadLogger.debug("in observer thread teleonomName=" + teleonomName + " teleonomAddress=" + teleonomAddress + " subscriber=" + subscriber );

							if(subscriber==null){
								aSubscriberThread = new SubscriberThread(teleonomAddress, teleonomName);
								aSubscriberThread.start();
								subscriberThreadCounter++;
								observerThreadLogger.debug("after creating subscriber subscriberThreadCounter=" + subscriberThreadCounter);

								if(teleonomesToReconnect.contains(teleonomName)){
									teleonomesToReconnect.remove(teleonomName);
								}

							}else{
								//
								// Now check to see how old is the last pulse, 
								// if the pulse is stale ie the last pulse is more than
								//the teleonome last current pulse, it could mean that
								// the ther teleonome went down and  since the 
								// subscriber has to be started before thepublisher
								// we need to kill the subscriber thread and restart it
								//

								observerThreadLogger.debug("teleonomName=" + teleonomName + " teleonomesToReconnect.contains(teleonomName)=" + teleonomesToReconnect.contains(teleonomName));

								if(teleonomesToReconnect.contains(teleonomName)){
									subscriberList.remove(teleonomAddress);
									subscriber=null;
									observerThreadLogger.debug(teleonomName + " with ip " +teleonomAddress + " needs to reconnect");

									teleonomesToReconnect.remove(teleonomName);
									logger.debug("after removing " + teleonomName +" teleonomesToReconnect="+ teleonomesToReconnect.size());
									aSubscriberThread = new SubscriberThread(teleonomAddress, teleonomName);
									aSubscriberThread.start();

								}

							}
						}



					}

					try {
						Thread.sleep(30000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						observerThreadLogger.warn(Utils.getStringException(e));
					}
				}catch(Exception e) {
					logger.warn(Utils.getStringException(e));
				}
			}while(true);

		}
	}
	class SubscriberThread extends Thread{
		String teleonomeAddress="";
		String teleonomeName="";
		Socket subscriber;
		JSONObject whoAmI=null;
		JSONObject howAmI=null;
		
		String topic;
		Vector externalTeleonomeNamesVector;
		public SubscriberThread(String t, String n){
			teleonomeAddress=t;
			teleonomeName=n;
			
			subscriber = exoZeroContext.socket(ZMQ.SUB);
			subscriber.setRcvHWM(1);
			subscriber.connect("tcp://"+ teleonomeAddress +":5563"); 
			subscriberThreadLogger.debug("subscribed to " + teleonomeName + ":" + teleonomeAddress);
			subscriberList.put(teleonomeAddress, subscriber);
			subscriber.subscribe("Status".getBytes()); 
			subscriber.subscribe(("Remember_" + aDenomeManager.getDenomeName()).getBytes()); 

			externalTeleonomeNamesVector = aDenomeManager.getExternalTeleonomeNamesRequired();

			subscriberThreadLogger.debug("externalTeleonomeNamesVector=" + externalTeleonomeNamesVector);
		}

		public void run(){
			boolean learnMyHistory=false;
			Boolean B;
			String learnOtherHistoryTeleonomeName="";
			Identity learnOtherHistoryDeneActiveIdentity= null;
			boolean learnOtherHistory=false;
			Identity learnOtherHistoryTeleonomeNameIdentity=null;
			
			boolean pulseLate = false;
			
			String lastPulseTimestamp = "";
			Identity identity = null;
			String statusMessage = "";
			String bootstrapStatus = "";
			String operationMode = "";
			
			String identityPointer="";
			String tSatus= "";
			JSONArray ssidJSONArray = null;
			Identity learnMyHistoryDeneActiveIdentity;
			
			
			while(true){
				JSONObject jsonMessage = null;
				topic = subscriber.recvStr ().trim();
				String contents = subscriber.recvStr ().trim();
				
				 learnMyHistoryDeneActiveIdentity= new Identity("@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_INTERNAL + ":" +  TeleonomeConstants.DENECHAIN_MNEMOSYCONS + ":" + TeleonomeConstants.DENE_NAME_MNEMOSYCON_LEARN_MY_HISTORY +":" + TeleonomeConstants.DENEWORD_ACTIVE);
				 learnMyHistory=false;
				 learnOtherHistoryTeleonomeName="";
				try {
					B = (Boolean) aDenomeManager.getDeneWordAttributeByIdentity(learnMyHistoryDeneActiveIdentity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
					if(B!=null)learnMyHistory = B.booleanValue();
				} catch (InvalidDenomeException | JSONException e2) {
					// TODO Auto-generated catch block
					logger.warn(Utils.getStringException(e2));

				}

				 learnOtherHistoryDeneActiveIdentity= new Identity("@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_INTERNAL + ":" +  TeleonomeConstants.DENECHAIN_MNEMOSYCONS + ":" + TeleonomeConstants.DENE_NAME_MNEMOSYCON_LEARN_OTHER_HISTORY +":" + TeleonomeConstants.DENEWORD_ACTIVE);
				 learnOtherHistory=false;
				try { 
					B = (Boolean) aDenomeManager.getDeneWordAttributeByIdentity(learnOtherHistoryDeneActiveIdentity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
					if(B!=null) {
						learnOtherHistory=B.booleanValue();
						if(learnOtherHistory) {
							 learnOtherHistoryTeleonomeNameIdentity= new Identity("@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_INTERNAL + ":" +  TeleonomeConstants.DENECHAIN_MNEMOSYCONS + ":" + TeleonomeConstants.DENE_NAME_MNEMOSYCON_LEARN_OTHER_HISTORY +":" + TeleonomeConstants.DENE_NAME_MNEMOSYCON_LEARN_OTHER_HISTORY_TELEONOME);
							learnOtherHistoryTeleonomeName = (String) aDenomeManager.getDeneWordAttributeByIdentity(learnOtherHistoryTeleonomeNameIdentity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
						}
					}
				} catch (InvalidDenomeException | JSONException e2) {
					// TODO Auto-generated catch block
					subscriberThreadLogger.warn(Utils.getStringException(e2));

				}


				//	if(teleonomeName.equals("Ra")){
				long lastPulseTime=0;
				if(topic.equals("Status")) {

					try {
						 jsonMessage = new JSONObject(contents);
						 pulseLate = Utils.isPulseLate(jsonMessage);
						teleonomeNamePulseIsLateIndex.put(teleonomeName, new Boolean(pulseLate));
						aDenomeManager.updateExternalData(teleonomeName, jsonMessage);
						lastPulseTime = jsonMessage.getLong("Pulse Timestamp in Milliseconds");
						 lastPulseTimestamp = jsonMessage.getString("Pulse Timestamp");
						 identity = new Identity("@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_PURPOSE + ":" +  TeleonomeConstants.DENECHAIN_OPERATIONAL_DATA + ":" + TeleonomeConstants.DENE_TYPE_VITAL +":" + TeleonomeConstants.DENEWORD_OPERATIONAL_MODE);
						 statusMessage = (String)getDeneWordByIdentity(jsonMessage, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);

						identity = new Identity("@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_PURPOSE + ":" +  TeleonomeConstants.DENECHAIN_OPERATIONAL_DATA + ":" + TeleonomeConstants.DENE_TYPE_VITAL +":" + TeleonomeConstants.DENEWORD_OPERATIONAL_STATUS_BOOTSTRAP_EQUIVALENT);
						 bootstrapStatus = (String)getDeneWordByIdentity(jsonMessage, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
						subscriberThreadLogger.debug("received from#" + teleonomeName + "#" + teleonomeAddress  + "#" + lastPulseTimestamp + "#" + statusMessage + "#" + bootstrapStatus);
						if(bootstrapStatus==null)bootstrapStatus="success";
						identity = new Identity("@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_PURPOSE + ":" +  TeleonomeConstants.DENECHAIN_OPERATIONAL_DATA + ":" + TeleonomeConstants.DENE_TYPE_VITAL +":" + TeleonomeConstants.DENEWORD_STATUS);
						 operationMode = (String)getDeneWordByIdentity(jsonMessage, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);

						 identityPointer=teleonomeName;
						 tSatus= teleonomeName + " " +  TeleonomeConstants.EXTERNAL_DATA_STATUS_OK;
						try {
							FileUtils.write(new File("pulses/" + teleonomeName + ".pulse"), jsonMessage.toString(4));
							FileUtils.write(new File("tomcat/webapps/ROOT/" + teleonomeName + ".pulse"), jsonMessage.toString(4));

						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						//
						// check to see if this teleonome persist organism pulse
						//
						if(aPulseThread.isPersistenceOrganismPulses()) {
							aDBManager.storeOrganismPulse(teleonomeName,localIpAddress,contents,tSatus,  operationMode,  identityPointer, lastPulseTime);	
						}
						
						//
						// now check to see if any words need to be unwrapped
						String rememberedWordPointer;
						Object value;
						String valueType;
						Hashtable<String,ArrayList> deneWordsToRememberByTeleonome = aDenomeManager.getDeneWordsToRememberByTeleonome();
						subscriberThreadLogger.debug("deneWordsToRememberByTeleonome " + deneWordsToRememberByTeleonome );
						
						ArrayList teleonomeRememberedWordsArrayList = deneWordsToRememberByTeleonome.get(teleonomeName);
						subscriberThreadLogger.debug("for " + teleonomeName + " teleonomeRememberedWordsArrayList: " + teleonomeRememberedWordsArrayList );
						
						if(teleonomeRememberedWordsArrayList!=null && teleonomeRememberedWordsArrayList.size()>0) {
							TimeZone timeZone = aDenomeManager.getTeleonomeTimeZone();
							for( int i=0;i<teleonomeRememberedWordsArrayList.size();i++) {
								rememberedWordPointer = (String) teleonomeRememberedWordsArrayList.get(i);
								value = getDeneWordByIdentity(jsonMessage,new Identity(rememberedWordPointer), TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
								valueType = (String) getDeneWordByIdentity(jsonMessage, new Identity(rememberedWordPointer), TeleonomeConstants.DENEWORD_VALUETYPE_ATTRIBUTE);
								subscriberThreadLogger.debug("about to unwrap " + rememberedWordPointer + " with value:" + value  + " and valueType=" + valueType);
								aMnemosyneManager.unwrap(timeZone, teleonomeName, lastPulseTime, rememberedWordPointer, valueType,value);
							}
						}
						//
						// nw publish the name of the teleonome and the bootstrapStatus
						// 
						organismViewStatusInfoJSONObject.put(teleonomeName,bootstrapStatus);
						publishToHeart(TeleonomeConstants.HEART_TOPIC_ORGANISM_STATUS, organismViewStatusInfoJSONObject.toString());

						 ssidJSONArray = NetworkUtilities.getSSID(false);
						publishToHeart(TeleonomeConstants.HEART_TOPIC_AVAILABLE_SSIDS, ssidJSONArray.toString());
						//
						// now check to see if this subscriber is waiting for data,ie, its external data referencing this telenome is stale
						//
						boolean somebodyIsWating = isSomebodyWaitingForMe( aDenomeManager.getDenomeName(), jsonMessage);
						if(somebodyIsWating) {
							subscriberThreadLogger.info(teleonomeName + " is waiting for data from " + aDenomeManager.getDenomeName() + " restarting the exozero publisher");
							
							//stopExoZeroPublisher();
							//startExoZeroPublisher();
							//subscriberThreadLogger.info( "  restarted the exozero publisher");
						}else{
							subscriberThreadLogger.info(teleonomeName + " is NOT waiting for data from " + aDenomeManager.getDenomeName()  );
						}
						
						
					} catch (JSONException e1) {
						// TODO Auto-generated catch block
						subscriberThreadLogger.warn("invalid pulse received from " + teleonomeName + ":" + teleonomeAddress  + " " + contents);
					} catch (InvalidDenomeException e) {
						subscriberThreadLogger.debug("invalid pulse received from " + teleonomeName + ":" + teleonomeAddress  + " " + contents);
						// TODO Auto-generated catch block
						subscriberThreadLogger.warn(Utils.getStringException(e));
					}
				}else if((learnMyHistory || learnOtherHistory) && topic.startsWith("Remember_")) {
					try {
						jsonMessage = new JSONObject(contents);
						lastPulseTime = jsonMessage.getLong("Pulse Timestamp in Milliseconds");

					} catch (JSONException e) {
						// TODO Auto-generated catch block
						subscriberThreadLogger.warn(Utils.getStringException(e));

					}
					if((learnMyHistory && topic.equals("Remember_" + aDenomeManager.getDenomeName()))) {
						//
						// if we are here we are remembering data about this telenome that
						// comes from somewhere else
						if(!aDBManager.containsPulse(lastPulseTime)) {
							aDBManager.storePulse(lastPulseTime,contents);
							aDBManager.storeRemembered(lastPulseTime,"Pulse", teleonomeName, teleonomeAddress,"");
						}
					}else if(learnOtherHistory){
						String pulseReceivedTeleonomeName=null;
						try {
							pulseReceivedTeleonomeName = jsonMessage.getJSONObject("Denome").getString("Name");
							
						} catch (JSONException e1) {
							// TODO Auto-generated catch block
							subscriberThreadLogger.warn(Utils.getStringException(e1));

						}
						if(learnOtherHistoryTeleonomeName.equals(pulseReceivedTeleonomeName)) {
							//
							// if we are here is because we are remembering data from other teleonomes
							try {
								 identity = new Identity("@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_PURPOSE + ":" +  TeleonomeConstants.DENECHAIN_OPERATIONAL_DATA + ":" + TeleonomeConstants.DENE_TYPE_VITAL +":" + TeleonomeConstants.DENEWORD_OPERATIONAL_MODE);
								 operationMode = (String)getDeneWordByIdentity(jsonMessage, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
								 identityPointer=teleonomeName;
								 tSatus= teleonomeName + " " +  TeleonomeConstants.EXTERNAL_DATA_STATUS_OK;
								if(!aDBManager.containsOrganismPulse(lastPulseTime, learnOtherHistoryTeleonomeName)) {
									aDBManager.storeOrganismPulse(teleonomeName,localIpAddress,contents,tSatus,  operationMode,  identityPointer, lastPulseTime);
									aDBManager.storeRemembered(lastPulseTime,"Organism", teleonomeName, teleonomeAddress,learnOtherHistoryTeleonomeName);
								}
							} catch (InvalidDenomeException e) {
								subscriberThreadLogger.debug("invalid pulse received from " + teleonomeName + ":" + teleonomeAddress  + " " + contents);
								// TODO Auto-generated catch block
								subscriberThreadLogger.warn(Utils.getStringException(e));
							}
						}
					}
				}
				jsonMessage=null;
				contents=null;
				System.gc();
			}
		}
		
		
		/**
		 * This method is used by the subscriber thread, to detect if there is a problem with
		 * the exozero network, ie if there is another teleonome waiting for data from this 
		 * teleonome.
		 * if it returns true then the exozero publisher needs to be restarted
		 * There are two places to check, the external data and all the mnemosycons of type DENE_TYPE_MNEMOSYCON_DENEWORDS_TO_REMEMBER
		 * 
		 * @param publisherTeleonomeName - the name of the publisher teleonome
		 * @param dependentTeleonomePulse - the data of the teleonome dependind of the publisherteleonome data
		 * @return
		 */
		public boolean isSomebodyWaitingForMe(String publisherTeleonomeName, JSONObject dependentTeleonomePulse){
			//
			// get the address of the deneword where this data is going to
			String reportingAddress, deneWordName;
			Vector teleonomeToReconnect = new Vector();
			boolean somebodyIsWating=false;
			try {
				
				JSONObject dependentPulseDenome = dependentTeleonomePulse.getJSONObject("Denome");
				String dependentTeleonomeName = dependentPulseDenome.getString("Name");
				JSONArray dependentPulseNuclei = dependentPulseDenome.getJSONArray("Nuclei");
				JSONArray deneWords;

				JSONObject jsonObject, jsonObjectChain, jsonObjectDene, jsonObjectDeneWord;
				JSONArray chains, denes;
				String externalDataDeneName;
				JSONObject lastPulseExternalTeleonomeJSONObject;
				String externalSourceOfData;

				
				long lastPulseExternalTimeInMillis,difference;
				String lastPulseExternalTime;
				Identity externalDataCurrentPulseIdentity,numberOfPulseForStaleIdentity;
				int secondsToStale=180;
				//String valueType;

				for(int i=0;i<dependentPulseNuclei.length();i++){
					jsonObject = dependentPulseNuclei.getJSONObject(i);
					if(jsonObject.getString("Name").equals(TeleonomeConstants.NUCLEI_PURPOSE)){
						chains = jsonObject.getJSONArray("DeneChains");
						for(int j=0;j<chains.length();j++){
							jsonObjectChain = chains.getJSONObject(j);

							if(jsonObjectChain.toString().length()>10 && jsonObjectChain.getString("Name").equals(TeleonomeConstants.DENECHAIN_EXTERNAL_DATA)){
								denes = jsonObjectChain.getJSONArray("Denes");

								for(int k=0;k<denes.length();k++){
									jsonObjectDene = denes.getJSONObject(k);
									externalDataDeneName = jsonObjectDene.getString("Name");
									//
									// the externalDataDeneName is the name of the External Teleonome
									// lastPulseExternalTeleonomeJSONObject contains the last pulse
									// of that teleonome
									//
									logger.debug("looking for  " + externalDataDeneName);
									if(publisherTeleonomeName.equals(externalDataDeneName)) {
								
										String externalDeneStatus=TeleonomeConstants.EXTERNAL_DATA_STATUS_STALE;
										Identity denewordStatusIdentity = new Identity(dependentTeleonomeName,TeleonomeConstants.NUCLEI_PURPOSE, TeleonomeConstants.DENECHAIN_EXTERNAL_DATA,publisherTeleonomeName, TeleonomeConstants.EXTERNAL_DATA_STATUS);
										try {
											externalDeneStatus = (String) getDeneWordByIdentity(dependentTeleonomePulse, denewordStatusIdentity,  TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
										} catch (InvalidDenomeException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
										logger.debug("externalDeneStatus after getting data by pointer " + externalDeneStatus);
										if(externalDeneStatus.equals(TeleonomeConstants.EXTERNAL_DATA_STATUS_STALE)) {
											somebodyIsWating=true;
										}
										
									}
								
									
								}
							}
						}
					}
				}
				//
				// now check the mnemosycons of denetype DENE_TYPE_MNEMOSYCON_DENEWORDS_TO_REMEMBER
				//
				
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				logger.warn(Utils.getStringException(e));
			}
			dependentTeleonomePulse=null;
			return somebodyIsWating;
		}
		
		public  Object getDeneWordByIdentity(JSONObject dataSource, Identity identity, String whatToBring) throws InvalidDenomeException{
			JSONArray deneChainsArray=null;
			Object toReturn=null;
			try {

				String nucleusName=identity.getNucleusName();
				String deneChainName=identity.getDenechainName();
				String deneName=identity.getDeneName();
				String deneWordName=identity.getDeneWordName();
				
				//	//System.out.println("poijbt 1");
				//
				// now parse them
				JSONObject denomeObject = dataSource.getJSONObject("Denome");
				JSONArray nucleiArray = denomeObject.getJSONArray("Nuclei");
				String name;
				JSONObject aJSONObject, internalNucleus = null,purposeNucleus = null,mnemosyneNucleus=null, humanInterfaceNucleus=null;
				//	//System.out.println("poijbt 2");
				for(int i=0;i<nucleiArray.length();i++){
					aJSONObject = (JSONObject) nucleiArray.get(i);
					name = aJSONObject.getString("Name");
					if(name.equals(TeleonomeConstants.NUCLEI_INTERNAL)){
						internalNucleus= aJSONObject;
						deneChainsArray = internalNucleus.getJSONArray("DeneChains");
					}else if(name.equals(TeleonomeConstants.NUCLEI_PURPOSE)){
						purposeNucleus= aJSONObject;
						deneChainsArray = purposeNucleus.getJSONArray("DeneChains");
					}else if(name.equals(TeleonomeConstants.NUCLEI_MNEMOSYNE)){
						mnemosyneNucleus= aJSONObject;
					}else if(name.equals(TeleonomeConstants.NUCLEI_HUMAN_INTERFACE)){
						humanInterfaceNucleus= aJSONObject;
					}

				}
				//	//System.out.println("poijbt 3");
				if(nucleusName.equals(TeleonomeConstants.NUCLEI_INTERNAL)){
					deneChainsArray = internalNucleus.getJSONArray("DeneChains");
				}else if(nucleusName.equals(TeleonomeConstants.NUCLEI_PURPOSE)){
					deneChainsArray = purposeNucleus.getJSONArray("DeneChains");
				}else if(nucleusName.equals(TeleonomeConstants.NUCLEI_MNEMOSYNE)){
					deneChainsArray = mnemosyneNucleus.getJSONArray("DeneChains");
				}else if(nucleusName.equals(TeleonomeConstants.NUCLEI_HUMAN_INTERFACE)){
					deneChainsArray = humanInterfaceNucleus.getJSONArray("DeneChains");
				}
				//	//System.out.println("poijbt 4");
				JSONObject aDeneJSONObject, aDeneWordJSONObject;
				JSONArray denesJSONArray, deneWordsJSONArray;
				String valueType, valueInString;
				Object object;
				for(int i=0;i<deneChainsArray.length();i++){
					aJSONObject = (JSONObject) deneChainsArray.get(i);
					if(aJSONObject.getString("Name").equals(deneChainName)){
						denesJSONArray = aJSONObject.getJSONArray("Denes");
						for(int j=0;j<denesJSONArray.length();j++){
							aDeneJSONObject = (JSONObject) denesJSONArray.get(j);
							//	//System.out.println("poijbt 5");
							if(aDeneJSONObject.getString("Name").equals(deneName)){
								deneWordsJSONArray = aDeneJSONObject.getJSONArray("DeneWords");
								for(int k=0;k<deneWordsJSONArray.length();k++){
									
									aDeneWordJSONObject = (JSONObject) deneWordsJSONArray.get(k);
									//subscriberThreadLogger.debug("aDeneWordJSONObject=" + aDeneWordJSONObject.getString("Name") + " deneWordName=" + deneWordName);
									if(aDeneWordJSONObject.getString("Name").equals(deneWordName)){
										//	//System.out.println("poijbt 7");
										if(whatToBring.equals(TeleonomeConstants.COMPLETE)){
											toReturn= aDeneWordJSONObject;
										}else{
											toReturn= aDeneWordJSONObject.get(whatToBring);
										}
									}
								}
							}
						}

					}
				}
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				// TODO Auto-generated catch block
				Hashtable info = new Hashtable();

				String m = "The denome is not formated Correctly. Error:" + e.getMessage() +" Stacktrace:" + ExceptionUtils.getStackTrace(e);
				info.put("message", m);
				throw new InvalidDenomeException(info);
			}
			dataSource=null;
			return toReturn;
		}
		
	}
	
	
	




	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args.length>0 && args[0].equals("-v")) {
			System.out.println("Medula Build " + BUILD_NUMBER);
		}else {
			new  TeleonomeHypothalamus();
		}
		
		
	}
}

