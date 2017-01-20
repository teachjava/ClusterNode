package com.miked;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.UUID;

public class Connection {
    private static int uuid_inc = 10;
    private static int messageNumber = 1000;

    private MulticastSocket inSocket;
    private MulticastSocket outSocket;

    //Use this if you are going to run this in different JVMs or on different machines.
    private String OUT_UUID = UUID.randomUUID().toString();
    //private String OUT_UUID = Integer.toString(uuid_inc++);

    private String INET_ADDR = "239.0.1.0";
    private InetAddress inetAddress;
    private int INET_PORT = 4446;

    private String proposedMasterNodeUUID;
    private int proposedMasterNodeMessageNumber;
    private boolean lockForMasterNode;
    private boolean thereIsAMaster;

    public Connection() {
        buildSockets();
    }

    public boolean iCanBecomeMaster() {
        int currentMessageNumber = messageNumber++;
        if (!thereIsAMaster) {
            if (becomeMaster(currentMessageNumber)) {
                if (commit(currentMessageNumber)) {
                    return true;
                } else {
                    rollback(currentMessageNumber);
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    private boolean becomeMaster(int currentMessageNumber) {
        Message message = new Message(OUT_UUID, Message.MessageType.BECOME_MASTER, currentMessageNumber);
        if (ClusterUtils.debug)
            System.out.println(OUT_UUID + " Send BecomeMaster:\n  SendMessage: " + message);
        send(outSocket, message);
        //return true;
        return receiveAll(message);
    }

    private boolean commit(int currentMessageNumber) {
        Message message = new Message(OUT_UUID, Message.MessageType.COMMIT, currentMessageNumber);
        if (ClusterUtils.debug)
            System.out.println(OUT_UUID + " Send Commit:\n  SendMessage: " + message);
        send(outSocket, message);
        return receiveAll(message);
    }

    private void rollback(int currentMessageNumber) {
        Message message = new Message(OUT_UUID, Message.MessageType.ROLLBACK, currentMessageNumber);
        if (ClusterUtils.debug)
            System.out.println(OUT_UUID + " Send Rollback:\n  SendMessage: " + message);
        send(outSocket, message);
    }

    private void send(MulticastSocket socket, Message sendMessage) {
        try {
            DatagramPacket datagramPacket = new DatagramPacket(sendMessage.getBytes(), sendMessage.length(), inetAddress, INET_PORT);
            socket.send(datagramPacket);
        } catch (IOException e) {
        }
    }

    private boolean receiveAll(Message sentMessage) {
        long startTime = System.currentTimeMillis();

        //Wait for one second for all responses
        while (System.currentTimeMillis() - startTime < 1000) {
            try {
                Message receivedMessage = receive(outSocket);
                if (receivedMessage.getUUID().equals(sentMessage.getUUID()) &&
                        receivedMessage.getMessageNumber() == sentMessage.getMessageNumber() &&
                        receivedMessage.getMessageType() == Message.MessageType.NO) {
                    if (ClusterUtils.debug)
                        System.out.println(OUT_UUID + " In Receive All:\n  ReceiveAll:\n    Received NO Message: " + receivedMessage);
                    return false;
                } else if (receivedMessage.getUUID().equals(sentMessage.getUUID()) &&
                        receivedMessage.getMessageNumber() == sentMessage.getMessageNumber() &&
                        receivedMessage.getMessageType() == Message.MessageType.YES) {
                    if (ClusterUtils.debug)
                        System.out.println(OUT_UUID + " In Receive All:\n  ReceiveAll:\n    Received YES Message: " + receivedMessage);
                }
            } catch (IOException e) {
            }
        }
        return true;
    }

    private Message receive(MulticastSocket socket) throws IOException {
        byte buf[] = new byte[1024];
        DatagramPacket msgPacket = new DatagramPacket(buf, buf.length);

        socket.setSoTimeout(500);
        socket.receive(msgPacket);

        Message receiveMessage = new Message(msgPacket.getData(), msgPacket.getLength());
        return receiveMessage;
    }

    public void receiveEvents() {
        try {
            Message inMessage = receive(inSocket);
            //if(inMessage.getUUID().equals(OUT_UUID))
            //	return;
            if (inMessage.getMessageType() == Message.MessageType.BECOME_MASTER) {
                if (ClusterUtils.debug)
                    System.out.println(OUT_UUID + " In Received ReceiveEvents BECOME_MASTER:\n    Received Message: " + inMessage);
                handleBecomeMaster(inMessage);
            } else if (inMessage.getMessageType() == Message.MessageType.COMMIT) {
                if (ClusterUtils.debug)
                    System.out.println(OUT_UUID + " In Received ReceiveEvents COMMIT:\n    Received Message: " + inMessage);
                handleCommit(inMessage);
            } else if (inMessage.getMessageType() == Message.MessageType.ROLLBACK) {
                if (ClusterUtils.debug)
                    System.out.println(OUT_UUID + " In Received ReceiveEvents ROLLBACK:\n    Received Message: " + inMessage);
                handleRollback(inMessage);
            }
        } catch (IOException e) {
        }
    }

    private void handleBecomeMaster(Message inMessage) {
        if (!lockForMasterNode && !thereIsAMaster) {
            proposedMasterNodeUUID = inMessage.getUUID();
            proposedMasterNodeMessageNumber = inMessage.getMessageNumber();
            lockForMasterNode = true;
            if (ClusterUtils.debug)
                System.out.println(OUT_UUID + " In ReceiveEvents HandleBecomeMaster Send YES: " + toString() + "\n    inMessage: " + inMessage);
            send(inSocket, new Message(inMessage.getUUID(), Message.MessageType.YES, inMessage.getMessageNumber()));

        } else {
            if (ClusterUtils.debug)
                System.out.println(OUT_UUID + " In ReceiveEvents HandleBecomeMaster Send NO: " + toString() + "\n    inMessage: " + inMessage);
            send(inSocket, new Message(inMessage.getUUID(), Message.MessageType.NO, inMessage.getMessageNumber()));
        }
    }

    private void handleCommit(Message inMessage) {
        if (proposedMasterNodeUUID.equals(inMessage.getUUID()) &&
                proposedMasterNodeMessageNumber == inMessage.getMessageNumber() &&
                lockForMasterNode) {
            thereIsAMaster = true;
            if (ClusterUtils.debug)
                System.out.println(OUT_UUID + " In ReceiveEvents HandleCommit YES: \n" + toString() + "\n    inMessage: " + inMessage);
            send(inSocket, new Message(inMessage.getUUID(), Message.MessageType.YES, inMessage.getMessageNumber()));
        } else {
            if (ClusterUtils.debug)
                System.out.println(OUT_UUID + " In ReceiveEvents HandleCommit NO: \n" + toString() + "\n    inMessage: " + inMessage);
            send(inSocket, new Message(inMessage.getUUID(), Message.MessageType.NO, inMessage.getMessageNumber()));
        }
    }

    private void handleRollback(Message inMessage) {
        if (proposedMasterNodeUUID.equals(inMessage.getUUID()) &&
                proposedMasterNodeMessageNumber == inMessage.getMessageNumber()) {
            proposedMasterNodeUUID = "";
            proposedMasterNodeMessageNumber = 0;
            lockForMasterNode = false;
            thereIsAMaster = false;
            if (ClusterUtils.debug)
                System.out.println("    " + OUT_UUID + " In ReceiveEvents HandleRollback: " + toString() + "\n    inMessage: " + inMessage);
        }
    }

    private void buildSockets() {
        try {
            inetAddress = InetAddress.getByName(INET_ADDR);
            inSocket = new MulticastSocket(INET_PORT);
            outSocket = new MulticastSocket(INET_PORT);
            inSocket.joinGroup(inetAddress);
            outSocket.joinGroup(inetAddress);
        } catch (IOException e) {
            System.out.println("    " + OUT_UUID + ": Usage: start java -cp . -Djava.net.preferIPv4Stack=true ClusterNode 10");
            System.exit(1);
        }
    }

    public String toString() {
        return "\n    Connection:\n      proposedMasterNodeUUID: " + proposedMasterNodeUUID + "\n      proposedMasterNodeMessageNumber: " + proposedMasterNodeMessageNumber +
                "\n      lockForMasterNode: " + lockForMasterNode + "\n      thereIsAMaster: " + thereIsAMaster;
    }

    public boolean isThereIsAMaster() {
        return thereIsAMaster;
    }

    public String getOUT_UUID() {
        return OUT_UUID;
    }
}
