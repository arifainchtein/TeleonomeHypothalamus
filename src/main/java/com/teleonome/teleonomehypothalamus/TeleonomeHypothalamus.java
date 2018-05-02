package com.teleonome.teleonomehypothalamus;


import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;
import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.teleonome.framework.TeleonomeConstants;
import com.teleonome.framework.denome.Identity;
import com.teleonome.framework.denome.DenomeUtils;
import com.teleonome.framework.exception.InvalidDenomeException;
import com.teleonome.framework.hypothalamus.Hypothalamus;
import com.teleonome.framework.hypothalamus.PulseThread;
import com.teleonome.framework.network.NetworkUtilities;
import com.teleonome.framework.utils.Utils;

public class TeleonomeHypothalamus extends Hypothalamus{

	JSONObject organismViewStatusInfoJSONObject = new JSONObject();
	public final static Logger observerThreadLogger = Logger.getLogger(TeleonomeHypothalamus.class.getName() + "." + ObserverThread.class.getSimpleName());
	public final static Logger subscriberThreadLogger = Logger.getLogger(TeleonomeHypothalamus.class.getName() + "." + SubscriberThread.class.getSimpleName());
	public final static String BUILD_NUMBER="02/05/2018 12:27";
	PulseThread aPulseThread = new PulseThread(this);
	Hashtable teleonomeNamePulseIsLateIndex = new Hashtable();
	
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
		public ObserverThread(){
			String bonjourServiceType = "_teleonome._tcp.local.";


			try {
				observerThreadLogger.info("about to create teleonome service");
				mdnsServer = JmDNS.create(getIpAddress());
				// Register a test service.
				ServiceInfo testService = ServiceInfo.create(bonjourServiceType, hostName, 6666, "Teleonome service");
				mdnsServer.registerService(testService);
				logger.info("created teleonome service");

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
			String thisTeleonomeName = aDenomeManager.getDenomeName();
			SubscriberThread aSubscriberThread;	
			do{
				try {
					Hashtable presentTeleonoms = aDiscoverTeleonoms.getPresentTeleonoms();
					Hashtable notPresentTeleonoms = aDiscoverTeleonoms.getNotPresentTeleonoms();
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

							String status = TeleonomeConstants.TELEONOME_STATUS_DISCOVERED;
							String operationMode=TeleonomeConstants.TELEONOME_OPERATION_MODE_UNKNOWN;
							String identity = TeleonomeConstants.TELEONOME_IDENTITY_ORGANISM;
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
		String contents="";
		String topic;
		Vector externalTeleonomeNamesVector;
		public SubscriberThread(String t, String n){
			teleonomeAddress=t;
			teleonomeName=n;
			
			subscriber = exoZeroContext.socket(ZMQ.SUB);
			subscriber.connect("tcp://"+ teleonomeAddress +":5563"); 
			subscriberThreadLogger.debug("subscribed to " + teleonomeName + ":" + teleonomeAddress);
			subscriberList.put(teleonomeAddress, subscriber);
			subscriber.subscribe("Status".getBytes()); 
			subscriber.subscribe(("Remember_" + aDenomeManager.getDenomeName()).getBytes()); 

			externalTeleonomeNamesVector = aDenomeManager.getExternalTeleonomeNamesRequired();

			subscriberThreadLogger.debug("externalTeleonomeNamesVector=" + externalTeleonomeNamesVector);
		}

		public void run(){

			while(true){
				topic = subscriber.recvStr ().trim();
				contents = subscriber.recvStr ().trim();
				Identity learnMyHistoryDeneActiveIdentity= new Identity("@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_INTERNAL + ":" +  TeleonomeConstants.DENECHAIN_MNEMOSYCONS + ":" + TeleonomeConstants.DENE_NAME_MNEMOSYCON_LEARN_MY_HISTORY +":" + TeleonomeConstants.DENEWORD_ACTIVE);
				boolean learnMyHistory=false;
				Boolean B;
				String learnOtherHistoryTeleonomeName="";
				try {
					B = (Boolean) aDenomeManager.getDeneWordAttributeByIdentity(learnMyHistoryDeneActiveIdentity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
					if(B!=null)learnMyHistory = B.booleanValue();
				} catch (InvalidDenomeException | JSONException e2) {
					// TODO Auto-generated catch block
					logger.warn(Utils.getStringException(e2));

				}

				Identity learnOtherHistoryDeneActiveIdentity= new Identity("@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_INTERNAL + ":" +  TeleonomeConstants.DENECHAIN_MNEMOSYCONS + ":" + TeleonomeConstants.DENE_NAME_MNEMOSYCON_LEARN_OTHER_HISTORY +":" + TeleonomeConstants.DENEWORD_ACTIVE);
				boolean learnOtherHistory=false;
				try { 
					B = (Boolean) aDenomeManager.getDeneWordAttributeByIdentity(learnOtherHistoryDeneActiveIdentity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
					if(B!=null) {
						learnOtherHistory=B.booleanValue();
						if(learnOtherHistory) {
							Identity learnOtherHistoryTeleonomeNameIdentity= new Identity("@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_INTERNAL + ":" +  TeleonomeConstants.DENECHAIN_MNEMOSYCONS + ":" + TeleonomeConstants.DENE_NAME_MNEMOSYCON_LEARN_OTHER_HISTORY +":" + TeleonomeConstants.DENE_NAME_MNEMOSYCON_LEARN_OTHER_HISTORY_TELEONOME);
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
						JSONObject jsonMessage = new JSONObject(contents);
						boolean pulseLate = Utils.isPulseLate(jsonMessage);
						teleonomeNamePulseIsLateIndex.put(teleonomeName, new Boolean(pulseLate));
						aDenomeManager.updateExternalData(teleonomeName, jsonMessage);
						lastPulseTime = jsonMessage.getLong("Pulse Timestamp in Milliseconds");
						String lastPulseTimestamp = jsonMessage.getString("Pulse Timestamp");
						Identity identity = new Identity("@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_PURPOSE + ":" +  TeleonomeConstants.DENECHAIN_OPERATIONAL_DATA + ":" + TeleonomeConstants.DENE_TYPE_VITAL +":" + TeleonomeConstants.DENEWORD_OPERATIONAL_MODE);
						String statusMessage = (String)DenomeUtils.getDeneWordByIdentity(jsonMessage, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);

						identity = new Identity("@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_PURPOSE + ":" +  TeleonomeConstants.DENECHAIN_OPERATIONAL_DATA + ":" + TeleonomeConstants.DENE_TYPE_VITAL +":" + TeleonomeConstants.DENEWORD_OPERATIONAL_STATUS_BOOTSTRAP_EQUIVALENT);
						String bootstrapStatus = (String)DenomeUtils.getDeneWordByIdentity(jsonMessage, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
						subscriberThreadLogger.debug("received from#" + teleonomeName + "#" + teleonomeAddress  + "#" + lastPulseTimestamp + "#" + statusMessage + "#" + bootstrapStatus);
						if(bootstrapStatus==null)bootstrapStatus="success";
						identity = new Identity("@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_PURPOSE + ":" +  TeleonomeConstants.DENECHAIN_OPERATIONAL_DATA + ":" + TeleonomeConstants.DENE_TYPE_VITAL +":" + TeleonomeConstants.DENEWORD_STATUS);
						String operationMode = (String)DenomeUtils.getDeneWordByIdentity(jsonMessage, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);

						String identityPointer=teleonomeName;
						String tSatus= teleonomeName + " " +  TeleonomeConstants.EXTERNAL_DATA_STATUS_OK;
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
						// nw publish the name of the teleonome and the bootstrapStatus
						//
						organismViewStatusInfoJSONObject.put(teleonomeName,bootstrapStatus);
						publishToHeart(TeleonomeConstants.HEART_TOPIC_ORGANISM_STATUS, organismViewStatusInfoJSONObject.toString());

						JSONArray ssidJSONArray = NetworkUtilities.getSSID(false);
						publishToHeart(TeleonomeConstants.HEART_TOPIC_AVAILABLE_SSIDS, ssidJSONArray.toString());


					} catch (JSONException e1) {
						// TODO Auto-generated catch block
						subscriberThreadLogger.warn("invalid pulse received from " + teleonomeName + ":" + teleonomeAddress  + " " + contents);
					} catch (InvalidDenomeException e) {
						subscriberThreadLogger.debug("invalid pulse received from " + teleonomeName + ":" + teleonomeAddress  + " " + contents);
						// TODO Auto-generated catch block
						logger.warn(Utils.getStringException(e));
					}
				}else if((learnMyHistory || learnOtherHistory) && topic.startsWith("Remember_")) {
					JSONObject jsonMessage=null;
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
							pulseReceivedTeleonomeName = DenomeUtils.getTeleonomeName(jsonMessage);
						} catch (JSONException e1) {
							// TODO Auto-generated catch block
							subscriberThreadLogger.warn(Utils.getStringException(e1));

						}
						if(learnOtherHistoryTeleonomeName.equals(pulseReceivedTeleonomeName)) {
							//
							// if we are here is because we are remembering data from other teleonomes
							try {
								Identity identity = new Identity("@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_PURPOSE + ":" +  TeleonomeConstants.DENECHAIN_OPERATIONAL_DATA + ":" + TeleonomeConstants.DENE_TYPE_VITAL +":" + TeleonomeConstants.DENEWORD_OPERATIONAL_MODE);
								String operationMode = (String)DenomeUtils.getDeneWordByIdentity(jsonMessage, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
								String identityPointer=teleonomeName;
								String tSatus= teleonomeName + " " +  TeleonomeConstants.EXTERNAL_DATA_STATUS_OK;
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
			}
		}
	}




	public static void main(String[] args) {
		// TODO Auto-generated method stub

		new TeleonomeHypothalamus();
	}
}
