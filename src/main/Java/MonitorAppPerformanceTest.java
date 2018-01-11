import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class MonitorAppPerformanceTest {
    private final static Client client = ClientBuilder.newClient();
    public static void main(String args[]) {
        while(true) {
            long startTime = System.currentTimeMillis();
            Response response = client.target("http://localhost:8084/oehpcs-monitor/v1/cluster/cluster1/topics/metrics").request(MediaType.APPLICATION_JSON_TYPE).get(Response.class);
            int statusCode = response.getStatus();
            System.out.println("Status is " + statusCode);
            long endTime = System.currentTimeMillis();
            System.out.println("Time taken is " + (endTime - startTime));
        }
    }
}
