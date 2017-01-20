import static com.miked.ClusterUtils.sleepLessThan;
import com.miked.*;

public class ClusterNode {

    private Connection connection = new Connection();

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: start java -cp . -Djava.net.preferIPv4Stack=true ClusterNode 10 ");
            return;
        }
        try {
            int loopTimes = Integer.parseInt(args[0]);
            sleepLessThan(10000);
            for (int i = 0; i < loopTimes; i++) {
                new ClusterNode();
            }
        } catch (NumberFormatException e) {
            System.out.println("Usage: start java -cp . -Djava.net.preferIPv4Stack=true ClusterNode 10");
            System.exit(1);
        }
    }

    public ClusterNode() {
        startListeningForEvents();
        startSendingEvents();
    }

    public boolean keepRunning() {
        return true;
        //Use this if you are only running on a single machine and want to exit once a println happens.
        //!ClusterNode.this.connection.isThereIsAMaster();
    }

    public void startSendingEvents() {
        Runnable sendTask = () -> {
            sleepLessThan(5000);
            while (keepRunning()) {
                if (ClusterNode.this.connection.iCanBecomeMaster()) {
                    System.out.println(">>>>>>>>> " + ClusterNode.this.connection.getOUT_UUID() + ": We are started! <<<<<<<<<<");
                }
                sleepLessThan(2000);
            }
            if (com.miked.ClusterUtils.debug)
                System.out.println(ClusterNode.this.connection.getOUT_UUID() + ": Stopping Send Events");
        };
        new Thread(sendTask).start();

    }

    public void startListeningForEvents() {
        Runnable receiveTask = () -> {
            while (keepRunning()) {
                ClusterNode.this.connection.receiveEvents();
            }
            if (com.miked.ClusterUtils.debug)
                System.out.println(ClusterNode.this.connection.getOUT_UUID() + ": Stopping Receiving Events");
        };
        new Thread(receiveTask).start();
    }
}

