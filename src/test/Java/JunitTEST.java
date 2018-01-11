import org.junit.Test;

public class JunitTEST {

    @Test
    public void Hello() {
        String string = null;
        System.out.println("yui" + string);
        if(getServicename() == null)
            System.out.println("yesss");
    }

    protected String getServicename() {
        String servicename = System.getProperty("servicename");
        return servicename;
    }
}
