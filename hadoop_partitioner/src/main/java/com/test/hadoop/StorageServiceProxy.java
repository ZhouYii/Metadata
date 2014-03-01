package com.test.hadoop;

import java.io.IOException;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.test.hadoop.AbstractJmxClient;

public class StorageServiceProxy extends AbstractJmxClient 
{
    private static final String ssObjName = "org.apache.cassandra.db:type=StorageService";
    private StorageServiceMBean ssProxy = null;
    private JMXConnection lJmxConn;
    
    public StorageServiceProxy(String host, Integer port, String username, String password)
			throws IOException {
		this(host, port, host, 9160, false, null, null);
	}
    
    public StorageServiceProxy(String host, int port, String thriftHost, int thriftPort, boolean thriftFramed, String username, String password)
    throws IOException
    {
        super(host, port, username, password);

        this.lJmxConn = new JMXConnection(host, port, username, password);
        // Setup the StorageService proxy.
        this.ssProxy = getSSProxy(this.lJmxConn.getMbeanServerConn());
    }
    
    public StorageServiceMBean getSSProxy(MBeanServerConnection mbeanConn)
    {
        StorageServiceMBean proxy = null;
        try
        {
            ObjectName name = new ObjectName(ssObjName);
            proxy = JMX.newMBeanProxy(mbeanConn, name, StorageServiceMBean.class);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
        return proxy;
    }

	public StorageServiceMBean getSSMB() {
		return this.ssProxy;
	}
    
}