/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Connections;

import Commons.variables;
import com.prosysopc.ua.ServiceException;
import com.prosysopc.ua.StatusException;
import com.prosysopc.ua.client.AddressSpaceException;
import com.prosysopc.ua.client.ServerConnectionException;
import com.prosysopc.ua.nodes.MethodArgumentException;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import msb.dds.DDSPublisher;
import msb.dds.MSB_DDS;
import msb.mqtt.MSB_MQTT;
import msb.opcua.OPCUAClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.opcfoundation.ua.builtintypes.Variant;

/**
 *
 * @author Admin
 */
public class Manager {
    
    VertxBus vertx_bus;
    
    public Manager(){
        vertx_bus = new VertxBus();
        
        Vertx.vertx().deployVerticle(vertx_bus);
        
        System.out.println("MSB Manager initialized.");
        
    }
    
    private HashMap<String, Object> requesters = new HashMap<String, Object>(); /*<device_id><mqtt Client, DataReader...>*/
    private HashMap<String, Object> senders = new HashMap<String, Object>(); /*<device_id><mqtt Client, DataWriter...>*/

    public Object getRequester(String device_id) {
        return requesters.get(device_id);
    }

    public Object getSender(String device_id) {
        return senders.get(device_id);
    }
    
    public void setRequester(String device_id, Object requester){
        requesters.put(device_id, requester);
    }
    
    public void setSender(String device_id, Object sender){
        senders.put(device_id, sender);
    }
    
    
    /**
     * Sends data (from agent) to a device
     */
    public boolean send_data(String device_id, String topic ,String msg) throws MqttException{
        
        ArrayList<String> myDeviceDescription = (ArrayList<String>) readDeviceInfo(device_id);
        String protocol = myDeviceDescription.get(1);
        
        System.out.println("Device's protocol: " + protocol);
       
        switch(protocol){
            case variables.dds_protocol:
                DDSPublisher pub = (DDSPublisher) senders.get(device_id);
                pub.publish_to_topic(topic, msg);
                
                
                return true;
            case variables.mqtt_protocol:
                MSB_MQTT msb = new msb.mqtt.MSB_MQTT("tcp://localhost:1883");
                msb.send_msg((MqttClient) senders.get(device_id), msg, /*qos*/0, topic);
                return true;
            case variables.opcua_protocol:
                OPCUAClient opcuaclnt = (OPCUAClient) senders.get(device_id);
                opcuaclnt.write_value(topic,msg);
                
                return true;
                
            default: 
                throw new IllegalArgumentException("Invalid protocol: " + protocol);
                      
        }        
    
    }
    
    /**
     * Requests data from a device:
     */
    public String request_data(String device_id, String topic , List<String> opt_args){
        
        ArrayList<String> myDeviceDescription = (ArrayList<String>) readDeviceInfo(device_id);
        String protocol = myDeviceDescription.get(1);
        
        switch(protocol){
            case variables.dds_protocol:
                
                return "not implemented yet.";
                
            case variables.mqtt_protocol:
                
                return "not implemented yet.";
            case variables.opcua_protocol:
                OPCUAClient opcuaclnt = (OPCUAClient) requesters.get(device_id);
                Variant[] result = null;
                try {
                //@TODO criar um topic management de OPCUA para verificar que tipo de variavel é (historico, method, variable...)
                //De momento so suporta a chamada de método
                result = opcuaclnt.callTrignometryMethod(opt_args.get(0), opt_args.get(1));
                } catch (ServiceException | StatusException | AddressSpaceException | ServerConnectionException | MethodArgumentException ex) {
                    Logger.getLogger(Manager.class.getName()).log(Level.SEVERE, null, ex);
                }

                return new String("result: " + result[0].getValue());
                
            default: 
                throw new IllegalArgumentException("Invalid protocol: " + protocol);
                      
        }        
    
    }

    public boolean sub_opc(String device_id){
        System.out.println("Webservice to subscribe called!");
        OPCUAClient opcuaclnt = (OPCUAClient) requesters.get(device_id);
        opcuaclnt.subscribe_to_fixed_nodeName(vertx_bus);
        return true;
    }
    
    /*
    * receives streaming data from device and makes it available through VertX topics.
    */
    public String receive_and_stream_data_from_device(String topic, String content){
        
        vertx_bus.send(topic, content);
        
        return null;
    }
    

    private static java.util.List<java.lang.String> readDeviceInfo(java.lang.String deviceId) {
        msbweb.RegisterDevice_Service service = new msbweb.RegisterDevice_Service();
        msbweb.DBInteraction port = service.getDBInteractionPort();
        return port.readDeviceInfo(deviceId);
    }

    
    
    
    
    
}
