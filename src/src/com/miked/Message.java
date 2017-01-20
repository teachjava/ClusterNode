package com.miked;

public class Message {
    private String UUID;
    private MessageType messageType;
    private String messageString;
    private int messageNumber;

    public enum MessageType {
        YES,
        NO,
        BECOME_MASTER,
        COMMIT,
        ROLLBACK
    }

    public Message(byte[] rawMessage, int messageLength) {
        parseMessageString(rawMessage, messageLength);
    }

    public Message(String UUID, MessageType messageType, int messageNumber) {
        this.UUID = UUID;
        this.messageType = messageType;
        this.messageNumber = messageNumber;
        buildMessageString();
    }

    public void buildMessageString() {
        messageString = UUID + ":" + messageNumber + ":" + messageType;
    }

    public String toString() {
        return "\n    Message: \n      UUID: " + UUID + "\n      messageNumber: " + messageNumber + "\n      messageType: " + messageType;
    }

    public void parseMessageString(byte[] rawMessage, int messageLength) {
        messageString = new String(rawMessage, 0, messageLength);

        String delimiters = "[:]";
        String[] tokens = messageString.split(delimiters);

        UUID = tokens[0];

        messageNumber = Integer.parseInt(tokens[1]);
        messageType = MessageType.valueOf(tokens[2]);
    }


    public int getMessageNumber() {
        return messageNumber;
    }

    public byte[] getBytes() {
        return messageString.getBytes();
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public String getUUID() {
        return UUID;
    }

    public int length() {
        return messageString.length();
    }

}
