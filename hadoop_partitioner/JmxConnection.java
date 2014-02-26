

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

class JMXConnection
{
    private static final String FMT_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
    private final String host, username, password;
    private final int port;
    private JMXConnector jmxc;
    private MBeanServerConnection mbeanServerConn;

    JMXConnection(String host, int port) throws IOException
    {
        this(host, port, null, null);
    }

    JMXConnection(String host, int port, String username, String password) throws IOException
    {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        connect();
    }

    private void connect() throws IOException
    {
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(FMT_URL, host, port));
        Map<String, Object> env = new HashMap<String, Object>();

        if (username != null)
            env.put(JMXConnector.CREDENTIALS, new String[]{ username, password });

        jmxc = JMXConnectorFactory.connect(jmxUrl, env);
        mbeanServerConn = jmxc.getMBeanServerConnection();
    }

    public void close() throws IOException
    {
        jmxc.close();
    }

    public MBeanServerConnection getMbeanServerConn()
    {
        return mbeanServerConn;
    }
}