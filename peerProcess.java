import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.Random;
import java.util.Date;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.nio.Buffer;
import java.util.List;
import java.util.ArrayList;

public class peerProcess {
    public static int peerId;
    public static ConcurrentHashMap<Integer, Socket> connectedPeers;

    public static void main(String args[]) {
        LinkedHashMap<Integer,peer> allPeersMap = readPeerInfo();
        HashMap<String,String> commonMap = readCommon();

        peerId = Integer.parseInt(args[0]);
        createFileDirectories(allPeersMap);
        
        peer p = allPeersMap.get(peerId);
        System.out.println("Current Peer Information:");
        System.out.println("Peer Id: "+p.getPeerId());
        System.out.println("Hostname: "+p.getHostname());
        System.out.println("Listening Port: "+p.getListeningPort());
        System.out.println("Has File: "+p.getHasFile());

        int noOfCompleteNodes = 0;
        updateBitField(p,commonMap);
        if(p.hasFile) {
            readOriginalFile(p,commonMap);
            noOfCompleteNodes++;
        }
        byte[] handshakeMessageArr = handshakeMessage(p);

        try {
            connectedPeers = new  ConcurrentHashMap<Integer, Socket>();
            Connection con = new Connection(p,allPeersMap,handshakeMessageArr,commonMap,noOfCompleteNodes);
            ReceiveMessage rm = new ReceiveMessage(p,allPeersMap,handshakeMessageArr,commonMap,noOfCompleteNodes);
            con.start();
            rm.start();
            Unchoking uc = new Unchoking(p, commonMap, allPeersMap, noOfCompleteNodes);
            uc.start();
            OptUnchoking opt = new OptUnchoking(p, commonMap, allPeersMap, noOfCompleteNodes);
            opt.start();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    /*Create a peer node */
    public static peer createPeer(int peerId, String hostname, int listeningPort, Boolean hasFile) {
        peer p = new peer(peerId,hostname,listeningPort,hasFile);
        return p;
    }

    /*Read PeerInfo.cfg file */
    public static LinkedHashMap<Integer,peer> readPeerInfo() {
        BufferedReader br;
        LinkedHashMap<Integer,peer> allPeersMap = new LinkedHashMap<Integer,peer>();
        try {
            br = new BufferedReader(new FileReader(Constants.PEERINFO_FILE));
            String line = br.readLine();
            while(line != null) {
                String[] lineArray = line.split(" ");
                peer p = createPeer(Integer.parseInt(lineArray[0]), lineArray[1], Integer.parseInt(lineArray[2]), !(Integer.parseInt(lineArray[3]) == 0));
                allPeersMap.put(p.peerId,p);
                line = br.readLine();
            }
            br.close();    
        } catch(FileNotFoundException f) {
            System.err.println(Constants.PEERINFO_FILE + " file not found!");
        } catch(IOException e) {
            e.printStackTrace();
        }
        return allPeersMap;
    }

    /*Read Common.cfg file */
    public static HashMap<String,String> readCommon() {
        BufferedReader br;
        HashMap<String,String> commonMap = new HashMap<String,String>();
        try {
            br = new BufferedReader(new FileReader(Constants.COMMON_FILE));
            String[] commonArray;
            String line = br.readLine();
            while(line != null) {
                commonArray = line.split(" ");
                commonMap.put(commonArray[0],commonArray[1]);
                line = br.readLine();
            }
        } catch(FileNotFoundException f) {
            System.err.println(Constants.COMMON_FILE + " file not found!");
        } catch(IOException e) {
            e.printStackTrace();
        }
        return commonMap;
    }

    /*Create peer file directories */
    public static void createFileDirectories(HashMap<Integer,peer> allPeersMap) {
        for(int peerId : allPeersMap.keySet()) {
            new File(Constants.PEER_PREFIX + peerId).mkdir();
        }
    }

    /*Update bitfield attribute of peer */
    public static void updateBitField(peer p, HashMap<String,String> commonMap) {
        int fileSize = Integer.parseInt(commonMap.get(Constants.FILE_SIZE));
        int pieceSize = Integer.parseInt(commonMap.get(Constants.PIECE_SIZE));
        float totalPiecesDec = (float) fileSize / pieceSize;
        int totalPieces = (int) Math.ceil(totalPiecesDec);
        int bitfield[] = new int[totalPieces];

        if(p.hasFile) {
            for(int i=0; i<bitfield.length;i++) {
                bitfield[i] = 1;
            }
        } else {
            for(int i=0; i<bitfield.length;i++) {
                bitfield[i] = 0;
            }
        }
        p.setBitfield(bitfield);
    }

    /*Read original file and update File Bytes */
    public static void readOriginalFile(peer p, HashMap<String,String> commonMap) {
        int fileSize = Integer.parseInt(commonMap.get(Constants.FILE_SIZE));
        int pieceSize = Integer.parseInt(commonMap.get(Constants.PIECE_SIZE));
        float totalPiecesDec = (float) fileSize / pieceSize;
        int totalPieces = (int) Math.ceil(totalPiecesDec);
        byte fileBytes[][] = new byte[totalPieces][];

        try {
            Path pathName = Paths.get(Constants.PEER_PREFIX + peerId + File.separator + commonMap.get(Constants.FILE_NAME));
            byte originalTotalBytes[] = Files.readAllBytes(pathName);
            int ptr = 0;
            for(int i=0;i<fileBytes.length;i++) {
                if(ptr + pieceSize <= fileSize) {
                    fileBytes[i] = Arrays.copyOfRange(originalTotalBytes, ptr, ptr + pieceSize);
                } else {
                    fileBytes[i] = Arrays.copyOfRange(originalTotalBytes, ptr, fileSize);
                }
                ptr = ptr + pieceSize;
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /*Creates a handshake message */
    public static byte[] handshakeMessage(peer p) {
        byte fullMessage[] = new byte[32];
        int ptr = 0;

        String headerString = "P2PFILESHARINGPROJ";
        byte headerBytes[] = headerString.getBytes();
        for(int i=0; i<headerBytes.length;i++) {
            fullMessage[ptr] = headerBytes[i];
            ptr++;
        }

        String zeroString = "0000000000";
        byte zeroBytes[] = zeroString.getBytes();
        for(int i=0; i<zeroBytes.length;i++) {
            fullMessage[ptr] = zeroBytes[i];
            ptr++;
        }

        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(p.getPeerId());
        byte peerIdBytes[] = bb.array();
        for(int i=0; i<peerIdBytes.length;i++) {
            fullMessage[ptr] = peerIdBytes[i];
            ptr++;
        }

        return fullMessage;
    }

    /*Creates a choke message */
    public static byte[] chokeMessage() {
        int messageType = 0;
        byte fullMessage[] = new byte[5];
        byte lengthByte[] = ByteBuffer.allocate(4).putInt(1).array();
        int ptr = 0;
        for(int i=0; i<lengthByte.length;i++) {
            fullMessage[ptr] = lengthByte[i];
            ptr++;
        }
        fullMessage[ptr] = (byte) messageType;
        return fullMessage;
    }

    /*Creates an unchoke message */
    public static byte[] unchokeMessage() {
        int messageType = 1;
        byte fullMessage[] = new byte[5];
        byte lengthByte[] = ByteBuffer.allocate(4).putInt(1).array();
        int ptr = 0;
        for(int i=0; i<lengthByte.length;i++) {
            fullMessage[ptr] = lengthByte[i];
            ptr++;
        }
        fullMessage[ptr] = (byte) messageType;
        return fullMessage;
    }

    /*Creates an uninterested message */
    public static byte[] interestedMessage() {
        int messageType = 2;
        byte fullMessage[] = new byte[5];
        byte lengthByte[] = ByteBuffer.allocate(4).putInt(1).array();
        int ptr = 0;
        for(int i=0; i<lengthByte.length;i++) {
            fullMessage[ptr] = lengthByte[i];
            ptr++;
        }
        fullMessage[ptr] = (byte) messageType;
        return fullMessage;
    }

    /*Creates a not interested message */
    public static byte[] notInterestedMessage() {
        int messageType = 3;
        byte fullMessage[] = new byte[5];
        byte lengthByte[] = ByteBuffer.allocate(4).putInt(1).array();
        int ptr = 0;
        for(int i=0; i<lengthByte.length;i++) {
            fullMessage[ptr] = lengthByte[i];
            ptr++;
        }
        fullMessage[ptr] = (byte) messageType;
        return fullMessage;
    }

    /*creates a have message */
    public static byte[] haveMessage(int pieceIndex) {
        int messageType = 4;
        byte payloadByte[] = ByteBuffer.allocate(4).putInt(pieceIndex).array();
        byte fullMessage[] = new byte[payloadByte.length + 5];
        byte lengthByte[] = ByteBuffer.allocate(4).putInt(1 + payloadByte.length).array();
        int ptr = 0;
        for(int i=0; i<lengthByte.length;i++) {
            fullMessage[ptr] = lengthByte[i];
            ptr++;
        }
        fullMessage[ptr] = (byte) messageType;
        ptr++;
        for(int i=0; i<payloadByte.length; i++) {
            fullMessage[ptr] = payloadByte[i];
            ptr++;
        }
        return fullMessage;
    }

    /*Creates a bitfield message */
    public static byte[] bitfieldMessage(int[] bitfield) {
        int messageType = 5;
        int ptr = 0;
        byte payloadByte[] = new byte[bitfield.length * 4];
        for(int i=0; i<bitfield.length; i++) {
            byte bitfieldByte[] = ByteBuffer.allocate(4).putInt(bitfield[i]).array();
            for(int j=0; j<bitfieldByte.length;j++) {
                payloadByte[ptr] = bitfieldByte[j];
                ptr++;
            }
        }
        byte fullMessage[] = new byte[payloadByte.length + 5];
        byte lengthByte[] = ByteBuffer.allocate(4).putInt(1 + payloadByte.length).array();
        ptr = 0;
        for(int i=0; i<lengthByte.length;i++) {
            fullMessage[ptr] = lengthByte[i];
            ptr++;
        }
        fullMessage[ptr++] = (byte) messageType;
        for(int i=0; i<payloadByte.length; i++) {
            fullMessage[ptr] = payloadByte[i];
            ptr++;
        }
        return fullMessage;
    }

    /*Creates a request message */
    public static byte[] requestMessage(int pieceIndex) {
        int messageType = 6;
        byte payloadByte[] = ByteBuffer.allocate(4).putInt(pieceIndex).array();
        byte fullMessage[] = new byte[payloadByte.length + 5];
        byte lengthByte[] = ByteBuffer.allocate(4).putInt(1 + payloadByte.length).array();
        int ptr = 0;
        for(int i=0; i<lengthByte.length;i++) {
            fullMessage[ptr] = lengthByte[i];
            ptr++;
        }
        fullMessage[ptr] = (byte) messageType;
        ptr++;
        for(int i=0; i<payloadByte.length; i++) {
            fullMessage[ptr] = payloadByte[i];
            ptr++;
        }
        return fullMessage;
    }

    /*Creates a piece message */
    public static byte[] pieceMessage(byte[] pieceContent, int pieceIndex) {
        int messageType = 7;
        int payloadSize = pieceContent.length + 4;
        byte payloadByte[] = new byte[payloadSize];

        byte pieceIndexByte[] = ByteBuffer.allocate(4).putInt(pieceIndex).array();
        
        byte fullMessage[] = new byte[payloadSize + 5];
        byte lengthByte[] = ByteBuffer.allocate(4).putInt(1 + payloadByte.length).array();

        int ptr = 0;

        for(int i=0; i<lengthByte.length;i++) {
            fullMessage[ptr] = lengthByte[i];
            ptr++;
        }
        
        fullMessage[ptr] = (byte) messageType;
        ptr++;
        
        for(int i=0; i<pieceIndexByte.length;i++) {
            payloadByte[ptr] = pieceIndexByte[i];
            ptr++;
        }
        
        for(int i=0; i<pieceContent.length;i++) {
            payloadByte[ptr] = pieceContent[i];
            ptr++;
        }
        return fullMessage;
    }
    
    static class Connection extends Thread {
        peer p;
        HashMap<Integer,peer> allPeersMap;
        byte[] handshakeMessage;
        HashMap<String,String> commonMap;
        int noOfCompleteNodes;

        Connection() {}

        Connection(peer p, HashMap<Integer,peer> allPeersMap, byte[] handshakeMessage, HashMap<String,String> commonMap, int noOfCompleteNodes) {
            this.p = p;
            this.allPeersMap = allPeersMap;
            this.handshakeMessage = handshakeMessage;
            this.commonMap = commonMap;
            this.noOfCompleteNodes = noOfCompleteNodes;
        }

        @Override public void run() {
            try {
                Set<Integer> allPeersIdSet = allPeersMap.keySet();
                for(int pId : allPeersIdSet) {
                    System.out.println(pId);
                }
                for(int pId : allPeersIdSet) {
                    peer connectionPeer = allPeersMap.get(pId);
                    String hostname = connectionPeer.getHostname();
                    int listeningPort = connectionPeer.getListeningPort();

                    if(p.getPeerId() == pId) {
                        break;
                    }
                    Socket socket = new Socket(hostname,listeningPort);
                    
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.flush();
                    out.write(handshakeMessage);
                    out.flush();

                    Logs.TCPConnection(p, connectionPeer);
                    LocalDateTime ldt = LocalDateTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
                    String time = ldt.format(formatter);

                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    byte inputBytes[] = in.readAllBytes();

                    byte peerIdByteArr[] = Arrays.copyOfRange(inputBytes, 28, 32);
                    ByteBuffer peerIdBuffer = ByteBuffer.wrap(peerIdByteArr);
                    int connectorId = peerIdBuffer.getInt();

                    if(connectorId != pId) {
                        socket.close();
                    } else {
                        //start sending messages here. Start with bitfield
                        connectedPeers.put(connectorId, socket);
                        Logs.TCPConnected(p, connectionPeer);
                        ldt = LocalDateTime.now();
                        formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
                        time = ldt.format(formatter);
                        System.out.println("["+time+"]: Peer "+p.getPeerId()+" is connected from Peer "+connectionPeer.getPeerId() + ".");
                        new StartMessaging(p, connectionPeer, socket, commonMap, allPeersMap, noOfCompleteNodes).start();
                    }
                }
            } catch(ConnectException c) {
                System.err.println("Connection refused.");
            } catch(UnknownHostException unknownHost) {
                System.err.println("You are trying to connect to an unknown host!");
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }

    static class ReceiveMessage extends Thread {
        peer p;
        HashMap<Integer,peer> allPeersMap;
        byte[] handshakeMessage;
        HashMap<String,String> commonMap;
        int noOfCompleteNodes;

        ReceiveMessage() {}

        ReceiveMessage(peer p, HashMap<Integer,peer> allPeersMap, byte[] handshakeMessage, HashMap<String,String> commonMap, int noOfCompleteNodes) {
            this.p = p;
            this.allPeersMap = allPeersMap;
            this.handshakeMessage = handshakeMessage;
            this.commonMap = commonMap;
            this.noOfCompleteNodes = noOfCompleteNodes;
        }

        @Override
        public void run() {
            try {
                ServerSocket ss = new ServerSocket(p.getListeningPort());
                int noOfPeers = allPeersMap.size() - 1;
                while(allPeersMap.size() - 1 > connectedPeers.size()) {
                    Socket socket = ss.accept();
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    byte inputBytes[] = in.readAllBytes();

                    byte peerIdByteArr[] = Arrays.copyOfRange(inputBytes, 28, 32);
                    ByteBuffer peerIdBuffer = ByteBuffer.wrap(peerIdByteArr);
                    int connectorId = peerIdBuffer.getInt();

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.flush();
                    out.write(handshakeMessage);
                    out.flush();
                    Logs.TCPConnection(p, allPeersMap.get(connectorId));
                    LocalDateTime ldt = LocalDateTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
                    String time = ldt.format(formatter);
                    System.out.println("["+time+"]: Peer "+p.getPeerId()+" makes a connection to Peer "+connectorId + ".");
                    new StartMessaging(p, allPeersMap.get(connectorId), socket, commonMap, allPeersMap, noOfCompleteNodes).start();
                    connectedPeers.put(connectorId, socket);
                }
            } catch(ConnectException c) {
                System.err.println("Connection refused.");
            } catch(UnknownHostException unknownHost) {
                System.err.println("You are trying to connect to an unknown host!");
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }

    static class StartMessaging extends Thread {
        peer p;
        peer connectedPeer;
        Socket socket;
        HashMap<String,String> commonMap;
        HashMap<Integer,peer> allPeersMap;
        int noOfCompleteNodes;


        StartMessaging(peer p, peer connectedPeer, Socket s, HashMap<String,String> commonMap, HashMap<Integer,peer> allPeersMap, int noOfCompleteNodes) {
            this.p = p;
            this.connectedPeer = connectedPeer;
            this.socket = socket;
            this.commonMap = commonMap;
            this.allPeersMap = allPeersMap;
            this.noOfCompleteNodes = noOfCompleteNodes;
        }

        @Override
        public synchronized void run() {
            synchronized(this) {
                try{
                    long startTime;
                    long endTime;
                    long elapsedTime;
                    int[] bitfield = p.getBitfield();
                    byte[] bitfieldMessage = bitfieldMessage(bitfield);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.flush();
                    out.write(bitfieldMessage);
                    out.flush();

                    while(noOfCompleteNodes < allPeersMap.size()) {
                        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                        startTime = System.nanoTime();
                        byte inputBytes[] = in.readAllBytes();
                        endTime = System.nanoTime();
                        elapsedTime = (endTime - startTime) / 1000000000;

                        byte[] msgLenBytes =  Arrays.copyOfRange(inputBytes, 0, 4);
                        int msgLen = ByteBuffer.wrap(msgLenBytes).getInt();
                        
                        byte[] receivedMessage = new byte[msgLen];
                        int msgType = ByteBuffer.wrap((Arrays.copyOfRange(inputBytes, 4, 5))).getInt();
                        receivedMessage = Arrays.copyOfRange(inputBytes, 5, inputBytes.length);

                        if(msgType == 0) {
                            Logs.choke(p, connectedPeer);
                            LocalDateTime ldt = LocalDateTime.now();
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
                            String time = ldt.format(formatter);
                            System.out.println("["+time+"]: Peer "+p.getPeerId()+" is choked by Peer "+connectedPeer.getPeerId() + ".");
                            connectedPeer.isChoked = true;
                        } else if(msgType == 1) {
                            Logs.unchoke(p, connectedPeer);
                            LocalDateTime ldt = LocalDateTime.now();
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
                            String time = ldt.format(formatter);
                            System.out.println("["+time+"]: Peer "+p.getPeerId()+" is unchoked by Peer "+connectedPeer.getPeerId() + ".");
                            connectedPeer.isChoked = false;
                            int[] peerBitfield = p.getBitfield();
                            int[] connectedPeerBitfield = connectedPeer.getBitfield();
                            int variedIndex = -1;
                            if(!Arrays.equals(peerBitfield, connectedPeerBitfield)) {
                                while(variedIndex == -1) {
                                    Random rand = new Random();
                                    int ind = rand.nextInt(peerBitfield.length);
                                    if(peerBitfield[ind] == 0 && connectedPeerBitfield[ind] == 1) {
                                        variedIndex = ind;
                                        break;
                                    }
                                }
                                out.flush();
                                out.write(requestMessage(variedIndex));
                                out.flush();
                            }
                        } else if(msgType == 2) {
                            Logs.interestedReceived(p, connectedPeer);
                            LocalDateTime ldt = LocalDateTime.now();
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
                            String time = ldt.format(formatter);
                            System.out.println("["+time+"]: Peer "+p.getPeerId()+" received the 'interested' message from Peer "+connectedPeer.getPeerId() + ".");
                            connectedPeer.isInterested = true;
                        } else if(msgType == 3) {
                            Logs.notInterestedReceived(p, connectedPeer);
                            LocalDateTime ldt = LocalDateTime.now();
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
                            String time = ldt.format(formatter);
                            System.out.println("["+time+"]: Peer "+p.getPeerId()+" received the 'not interested' message from Peer "+connectedPeer.getPeerId() + ".");
                            connectedPeer.isInterested = false;
                            if(connectedPeer.isChoked == false) {
                                connectedPeer.isChoked = true;
                                out.flush();
                                out.write(chokeMessage());
                                out.flush();
                            }
                        } else if(msgType == 4) {
                            int[] connectBitField = connectedPeer.getBitfield();
                            int[] pBitField = p.getBitfield();
                            connectBitField[ByteBuffer.wrap(receivedMessage).getInt()] = 1;
                            connectedPeer.setBitfield(connectBitField);
                            if(!Arrays.asList(connectBitField).contains(0)) {
                                connectedPeer.hasFile = true;
                                noOfCompleteNodes++;
                            }
                            Boolean hasInterest = false;
                            int i=0;
                            while(i<connectBitField.length) {
                                if(pBitField[i] == 0 && connectBitField[i] == 1) {
                                    out.flush();
                                    out.write(interestedMessage());
                                    out.flush();
                                    hasInterest = true;
                                    break;
                                }
                                i++;
                            }
                            if(!hasInterest) {
                                out.flush();
                                out.write(notInterestedMessage());
                                out.flush();
                            }
                            Logs.haveReceived(p, connectedPeer, ByteBuffer.wrap(receivedMessage).getInt());
                            LocalDateTime ldt = LocalDateTime.now();
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
                            String time = ldt.format(formatter);
                            System.out.println("["+time+"]: Peer "+p.getPeerId()+" received the  'have' message from Peer "+connectedPeer.getPeerId() + " for the piece "+ByteBuffer.wrap(receivedMessage).getInt()+".");
                        } else if(msgType == 5) {
                            int intLength = receivedMessage.length/4;
                            int x = 0;
                            int ptr = 0;
                            int[] bitfieldArr = new int[intLength];
                            while(x < receivedMessage.length) {
                                byte[] bMsg = new byte[4];
                                for(int i=x; i < x+4; i++) {
                                    bMsg[i] = receivedMessage[i];
                                }
                                bitfieldArr[ptr] = ByteBuffer.wrap(bMsg).getInt();
                                ptr++;
                                x = x + 4;
                            }
                            connectedPeer.setBitfield(bitfieldArr);
                            if(!Arrays.asList(bitfieldArr).contains(0)) {
                                connectedPeer.hasFile = true;
                                noOfCompleteNodes++;
                            } else {
                                connectedPeer.hasFile = false;
                            }
                            int[] pBitField = p.getBitfield();
                            Boolean hasInterest = false;
                            int i=0;
                            while(i<bitfieldArr.length) {
                                if(pBitField[i] == 0 && bitfieldArr[i] == 1) {
                                    out.flush();
                                    out.write(interestedMessage());
                                    out.flush();
                                    hasInterest = true;
                                    break;
                                }
                                i++;
                            }
                            if(!hasInterest) {
                                out.flush();
                                out.write(notInterestedMessage());
                                out.flush();
                            }
                        } else if(msgType == 6) {
                            byte[] pieceBuffer = Arrays.copyOfRange(receivedMessage, 0, 4);
                            int i = ByteBuffer.wrap(pieceBuffer).getInt();
                            out.flush();
                            out.write(pieceMessage(connectedPeer.getFile()[i], i));
                            out.flush();
                        } else if(msgType == 7) {
                            byte[] pieceBuffer = Arrays.copyOfRange(receivedMessage, 0, 4);
                            int i = ByteBuffer.wrap(pieceBuffer).getInt();
                            int len = receivedMessage.length - 4;
                            p.getFile()[i] = new byte[len];
                            int x = 4;
                            int ptr = 0;
                            while(x < receivedMessage.length) {
                                p.getFile()[i][ptr] = receivedMessage[x];
                                x++;
                                ptr++;
                            }
                            int[] bitfieldArr = p.getBitfield();
                            bitfieldArr[i] = 1;
                            p.setBitfield(bitfieldArr);
                            if(!Arrays.asList(bitfieldArr).contains(0)) {
                                connectedPeer.hasFile = true;
                            }
                            if(!connectedPeer.isChoked) {
                                int[] connectedPeerBitfield = connectedPeer.getBitfield();
                                int variedIndex = -1;
                                if(!Arrays.equals(bitfieldArr, connectedPeerBitfield)) {
                                    while(variedIndex == -1) {
                                        Random rand = new Random();
                                        int ind = rand.nextInt(bitfieldArr.length);
                                        if(bitfieldArr[ind] == 0 && connectedPeerBitfield[ind] == 1) {
                                            variedIndex = i;
                                            break;
                                        }
                                    }
                                    out.flush();
                                    out.write(requestMessage(variedIndex));
                                    out.flush();
                                }
                            }
                            long speed = (long) (receivedMessage.length + 5) / elapsedTime;
                            connectedPeer.downloadSpeed = speed;
                            Logs.downloadPiece(p, connectedPeer, i, Logs.getNoOfPieces() + 1);

                            LocalDateTime ldt = LocalDateTime.now();
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
                            String time = ldt.format(formatter);
                            String msg = "["+time+"]: Peer "+p.getPeerId()+" has downloaded the piece " + i + "from Peer "+connectedPeer.getPeerId() + ". ";
                            System.out.print(msg);
                            msg = "Now the number of pieces it has is "+Logs.getNoOfPieces()+".";
                            System.out.println(msg);

                            byte[] fullFile = new byte[Integer.parseInt(commonMap.get("FileSize"))];
                            if(!Arrays.asList(bitfieldArr).contains(0)) {
                                connectedPeer.hasFile = true;
                                noOfCompleteNodes++;
                                ptr = 0;
                                for(int k=0; k< Logs.getNoOfPieces(); k++) {
                                    for(int j=0; j<p.getFile()[k].length; j++) {
                                        fullFile[ptr] = p.getFile()[k][j];
                                        ptr++;
                                    }
                                }
                                FileOutputStream fileOutputStream = new FileOutputStream("peer_" + p.getPeerId() + File.separatorChar + commonMap.get("FileName"));
                                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
                                bufferedOutputStream.write(fullFile);
                                bufferedOutputStream.close();
                                fileOutputStream.close();
                                Logs.downloadComplete(p);
                                ldt = LocalDateTime.now();
                                formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
                                time = ldt.format(formatter);
                                System.out.println("["+time+"]: Peer "+p.getPeerId()+" has downloaded the complete file.");
                            }
                            
                            for (int peerId : connectedPeers.keySet()) {
                                ObjectOutputStream out1 = new ObjectOutputStream(connectedPeers.get(peerId).getOutputStream());
                                out.flush();
                                out.write(haveMessage(i));
                                out.flush();
                            }
                        }
                    }
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class OptUnchoking extends Thread {
        peer p;
        HashMap<String,String> commonMap;
        HashMap<Integer,peer> allPeersMap;
        int noOfCompleteNodes;
        List<Integer> peersShowingInterest;

        OptUnchoking(peer p, HashMap<String,String> commonMap, HashMap<Integer,peer> allPeersMap, int noOfCompleteNodes) {
            this.p = p;
            this.commonMap = commonMap;
            this.allPeersMap = allPeersMap;
            this.noOfCompleteNodes = noOfCompleteNodes;
        }

        @Override
        public void run() {
            try {
                int totalNodes = allPeersMap.size();
                while(totalNodes > noOfCompleteNodes) {
                    Set<Integer> peerIdSet = connectedPeers.keySet();
                    List<Integer> peerIds = new ArrayList<>(peerIdSet);
                    peersShowingInterest = new ArrayList<>();
                    for(int i=0; i<peerIds.size(); i++) {
                        int id = peerIds.get(i);
                        peer newp = allPeersMap.get(id);
                        if(newp.isChoked && newp.isInterested) {
                            peersShowingInterest.add(id);
                        }
                    }
                    Random rand = new Random();
                    int pId = 0;
                    int pSize = peersShowingInterest.size();
                    if(pSize > 0) {
                        pId = peersShowingInterest.get(rand.nextInt(pSize));
                        ObjectOutputStream out = new ObjectOutputStream(connectedPeers.get(pId).getOutputStream());
                        out.flush();
                        out.write(unchokeMessage());
                        out.flush();
                        allPeersMap.get(pId).isChoked = false;
                    }
                    if(pId != 0) {
                        Logs.changeOptimisticallyUnchokedNeighbor(p, allPeersMap.get(pId));
                    }
                    long t = 1000L;
                    long oui = Long.parseLong(commonMap.get("OptimisticUnchokingInterval")) * t;
                    Thread.sleep(oui);
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    static class Unchoking extends Thread {
        peer p;
        HashMap<String,String> commonMap;
        HashMap<Integer,peer> allPeersMap;
        int noOfCompleteNodes;
        List<Integer> peersBeingPreferred;
        List<Integer> peersShowingInterest;
        
        Unchoking(peer p, HashMap<String,String> commonMap, HashMap<Integer,peer> allPeersMap, int noOfCompleteNodes) {
            this.p = p;
            this.commonMap = commonMap;
            this.allPeersMap = allPeersMap;
            this.noOfCompleteNodes = noOfCompleteNodes;
        }

        @Override
        public void run() {
            try {
                int totalNodes = allPeersMap.size();
                peersBeingPreferred = new ArrayList<>();
                peersShowingInterest = new ArrayList<>();
                while(totalNodes > noOfCompleteNodes) {
                    Set<Integer> peerIdSet = connectedPeers.keySet();
                    List<Integer> peerIds = new ArrayList<>(peerIdSet);
                    for(int i=0; i<peerIds.size(); i++) {
                        int id = peerIds.get(i);
                        peer newp = allPeersMap.get(id);
                        if(newp.isInterested) {
                            peersShowingInterest.add(id);
                        }
                    }
                    int totalIntPeers = peersShowingInterest.size();
                    int totalPrefPeers = Integer.parseInt(commonMap.get("NumberOfPreferredNeighbors"));
                    if(totalPrefPeers >= totalIntPeers) {
                        for(Integer i : peersShowingInterest) {
                            peersBeingPreferred.add(i);
                        }
                    } else {
                        if(!p.hasFile) {
                            long[] downloadSpeeds = new long[peersShowingInterest.size()];
                            for(int i=0; i<peersShowingInterest.size(); i++) {
                                peer newp = allPeersMap.get(peersShowingInterest.get(i));
                                downloadSpeeds[i] = newp.downloadSpeed;
                            }
                            
                            int peerForRemoval = 0;
                            for (int i = 1; i < downloadSpeeds.length; i++) {
                                peerForRemoval = downloadSpeeds[i] > downloadSpeeds[peerForRemoval] ? i : peerForRemoval;
                            }
                            peersBeingPreferred.add(peersShowingInterest.get(peerForRemoval));
                            peersShowingInterest.remove(peerForRemoval);
                        } else {
                            Random rand = new Random();
                            int i = 0;
                            while(i < totalPrefPeers) {
                                int size = peersShowingInterest.size();
                                int ind = rand.nextInt(size);
                                if(peersBeingPreferred.contains(ind) == false) {
                                    peersBeingPreferred.add(peersShowingInterest.get(ind));
                                    peersShowingInterest.remove(ind);
                                }
                                i++;
                            }
                        }
                    }
                    int ind = 0;
                    while(ind < peersBeingPreferred.size()) {
                        int prInd = peersBeingPreferred.get(ind);
                        peer pr = allPeersMap.get(prInd);
                        pr.isChoked = false;
                        ObjectOutputStream out = new ObjectOutputStream(connectedPeers.get(prInd).getOutputStream());
                        out.flush();
                        out.write(unchokeMessage());
                        out.flush();
                        ind++;
                    }
                    ind = 0;
                    while(ind < peersShowingInterest.size()) {
                        int prInd = peersShowingInterest.get(ind);
                        peer pr = allPeersMap.get(prInd);
                        if(pr.isChoked = false) {
                            pr.isChoked = true;
                            ObjectOutputStream out = new ObjectOutputStream(connectedPeers.get(prInd).getOutputStream());
                            out.flush();
                            out.write(chokeMessage());
                            out.flush();
                        }
                        ind++;
                    }
                    long t = 1000L;
                    long unchokingInterval = Long.parseLong(commonMap.get("UnchokingInterval")) * t;
                    Thread.sleep(unchokingInterval);
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
}

/*Class to create a peer object */
class peer {
    int peerId;
    String hostname;
    int listeningPort;
    Boolean hasFile;
    int[] bitfield;
    Boolean isChoked;
    Boolean isInterested;
    byte[][] file;
    long downloadSpeed;

    public peer(int peerId, String hostname, int listeningPort, Boolean hasFile) {
        this.peerId = peerId;
        this.hostname = hostname;
        this.listeningPort = listeningPort;
        this.hasFile = hasFile;
        this.isChoked = false;
        this.isInterested = false;
        this.downloadSpeed = 0;
    }

    public int getPeerId() {
        return this.peerId;
    }

    public void setPeerId(int peerId) {
        this.peerId = peerId;
    }

    public String getHostname() {
        return this.hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getListeningPort() {
        return this.listeningPort;
    }

    public void setListeningPort(int listeningPort) {
        this.listeningPort = listeningPort;
    }

    public Boolean isHasFile() {
        return this.hasFile;
    }

    public Boolean getHasFile() {
        return this.hasFile;
    }

    public void setHasFile(Boolean hasFile) {
        this.hasFile = hasFile;
    }

    public int[] getBitfield() {
        return this.bitfield;
    }

    public void setBitfield(int[] bitfield) {
        this.bitfield = bitfield;
    }

    public byte[][] getFile() {
        return this.file;
    }
}

/*class to create common attributes */
class Common {
    int NumberOfPreferredNeighbors;
    int UnchokingInterval;
    int OptimisticUnchokingInterval;
    String FileName;
    int FileSize;
    int PieceSize;

    public Common(int NumberOfPreferredNeighbors, int UnchokingInterval, int OptimisticUnchokingInterval, String FileName, int FileSize, int PieceSize) {
        this.NumberOfPreferredNeighbors = NumberOfPreferredNeighbors; 
        this.UnchokingInterval = UnchokingInterval;
        this.OptimisticUnchokingInterval = OptimisticUnchokingInterval;
        this.FileName = FileName;
        this.FileSize = FileSize;
        this.PieceSize = PieceSize;
    }

    public int getNumberOfPreferredNeighbors() {
        return this.NumberOfPreferredNeighbors;
    }

    public void setNumberOfPreferredNeighbors(int NumberOfPreferredNeighbors) {
        this.NumberOfPreferredNeighbors = NumberOfPreferredNeighbors;
    }

    public int getUnchokingInterval() {
        return this.UnchokingInterval;
    }

    public void setUnchokingInterval(int UnchokingInterval) {
        this.UnchokingInterval = UnchokingInterval;
    }

    public int getOptimisticUnchokingInterval() {
        return this.OptimisticUnchokingInterval;
    }

    public void setOptimisticUnchokingInterval(int OptimisticUnchokingInterval) {
        this.OptimisticUnchokingInterval = OptimisticUnchokingInterval;
    }

    public String getFileName() {
        return this.FileName;
    }

    public void setFileName(String FileName) {
        this.FileName = FileName;
    }

    public int getFileSize() {
        return this.FileSize;
    }

    public void setFileSize(int FileSize) {
        this.FileSize = FileSize;
    }

    public int getPieceSize() {
        return this.PieceSize;
    }

    public void setPieceSize(int PieceSize) {
        this.PieceSize = PieceSize;
    }    
}

/*Class to create log files */
class Logs {
    static BufferedWriter bw;
    static int noOfPieces = 0;

    /*Log into file when TCP connection requested */
    public static void TCPConnection(peer p1, peer p2) {
        File f = new File(Constants.LOG_PEER_PREFIX + p1.getPeerId());
        try {
            if (!f.exists()) {
                f.createNewFile();
            }
            FileWriter fw = new FileWriter(f);
            bw = new BufferedWriter(fw);
            LocalDateTime ldt = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
            String time = ldt.format(formatter);
            String msg = "["+time+"]: Peer "+p1.getPeerId()+" makes a connection to Peer "+p2.getPeerId() + ".";
            bw.append(msg);
            bw.newLine();
            bw.flush();
            bw.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /*Log into file when TCP connection made */
    public static void TCPConnected(peer p1, peer p2) {
        File f = new File(Constants.LOG_PEER_PREFIX + p1.getPeerId());
        try {
            if (!f.exists()) {
                f.createNewFile();
            }
            FileWriter fw = new FileWriter(f);
            bw = new BufferedWriter(fw);
            LocalDateTime ldt = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
            String time = ldt.format(formatter);
            String msg = "["+time+"]: Peer "+p1.getPeerId()+" is connected from Peer "+p2.getPeerId() + ".";
            bw.append(msg);
            bw.newLine();
            bw.flush();
            bw.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /*Log into file when preferred neighbour changed */
    public static void changePreferredNeighbors(peer p1, peer[] neighbors) {
        File f = new File(Constants.LOG_PEER_PREFIX + p1.getPeerId());
        try {
            if (!f.exists()) {
                f.createNewFile();
            }
            FileWriter fw = new FileWriter(f);
            bw = new BufferedWriter(fw);
            LocalDateTime ldt = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
            String time = ldt.format(formatter);
            String msg = "["+time+"]: Peer "+p1.getPeerId()+" has the preferred neighbours "+neighbors.toString() + ".";
            bw.append(msg);
            bw.newLine();
            bw.flush();
            bw.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /*Log into file when optimistically unchoked neighbour */
    public static void changeOptimisticallyUnchokedNeighbor(peer p1, peer p2) {
        File f = new File(Constants.LOG_PEER_PREFIX + p1.getPeerId());
        try {
            if (!f.exists()) {
                f.createNewFile();
            }
            FileWriter fw = new FileWriter(f);
            bw = new BufferedWriter(fw);
            LocalDateTime ldt = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
            String time = ldt.format(formatter);
            String msg = "["+time+"]: Peer "+p1.getPeerId()+" has the optimistically unchoked neighbor Peer "+p2.getPeerId() + ".";
            bw.append(msg);
            bw.newLine();
            bw.flush();
            bw.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /*Log into file when unchoked */
    public static void unchoke(peer p1, peer p2) {
        File f = new File(Constants.LOG_PEER_PREFIX + p1.getPeerId());
        try {
            if (!f.exists()) {
                f.createNewFile();
            }
            FileWriter fw = new FileWriter(f);
            bw = new BufferedWriter(fw);
            LocalDateTime ldt = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
            String time = ldt.format(formatter);
            String msg = "["+time+"]: Peer "+p1.getPeerId()+" is unchoked by Peer "+p2.getPeerId() + ".";
            bw.append(msg);
            bw.newLine();
            bw.flush();
            bw.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /*Log into file when choked */
    public static void choke(peer p1, peer p2) {
        File f = new File(Constants.LOG_PEER_PREFIX + p1.getPeerId());
        try {
            if (!f.exists()) {
                f.createNewFile();
            }
            FileWriter fw = new FileWriter(f);
            bw = new BufferedWriter(fw);
            LocalDateTime ldt = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
            String time = ldt.format(formatter);
            String msg = "["+time+"]: Peer "+p1.getPeerId()+" is choked by Peer "+p2.getPeerId() + ".";
            bw.append(msg);
            bw.newLine();
            bw.flush();
            bw.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /*Log into file when 'have' message received */
    public static void haveReceived(peer p1, peer p2, int pieceIndex) {
        File f = new File(Constants.LOG_PEER_PREFIX + p1.getPeerId());
        try {
            if (!f.exists()) {
                f.createNewFile();
            }
            FileWriter fw = new FileWriter(f);
            bw = new BufferedWriter(fw);
            LocalDateTime ldt = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
            String time = ldt.format(formatter);
            String msg = "["+time+"]: Peer "+p1.getPeerId()+" received the  'have' message from Peer "+p2.getPeerId() + " for the piece "+pieceIndex+".";
            bw.append(msg);
            bw.newLine();
            bw.flush();
            bw.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /*Log into file when 'interested' message received */
    public static void interestedReceived(peer p1, peer p2) {
        File f = new File(Constants.LOG_PEER_PREFIX + p1.getPeerId());
        try {
            if (!f.exists()) {
                f.createNewFile();
            }
            FileWriter fw = new FileWriter(f);
            bw = new BufferedWriter(fw);
            LocalDateTime ldt = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
            String time = ldt.format(formatter);
            String msg = "["+time+"]: Peer "+p1.getPeerId()+" received the 'interested' message from Peer "+p2.getPeerId() + ".";
            bw.append(msg);
            bw.newLine();
            bw.flush();
            bw.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /*Log into file when 'not interested' message received */
    public static void notInterestedReceived(peer p1, peer p2) {
        File f = new File(Constants.LOG_PEER_PREFIX + p1.getPeerId());
        try {
            if (!f.exists()) {
                f.createNewFile();
            }
            FileWriter fw = new FileWriter(f);
            bw = new BufferedWriter(fw);
            LocalDateTime ldt = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
            String time = ldt.format(formatter);
            String msg = "["+time+"]: Peer "+p1.getPeerId()+" received the 'not interested' message from Peer "+p2.getPeerId() + ".";
            bw.append(msg);
            bw.newLine();
            bw.flush();
            bw.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /*Log into file when piece downloaded */
    public static void downloadPiece(peer p1, peer p2, int pieceIndex, int noOfPieces) {
        File f = new File(Constants.LOG_PEER_PREFIX + p1.getPeerId());
        try {
            if (!f.exists()) {
                f.createNewFile();
            }
            FileWriter fw = new FileWriter(f);
            bw = new BufferedWriter(fw);
            LocalDateTime ldt = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
            String time = ldt.format(formatter);
            String msg = "["+time+"]: Peer "+p1.getPeerId()+" has downloaded the piece " + pieceIndex + "from Peer "+p2.getPeerId() + ". ";
            bw.append(msg);
            msg = "Now the number of pieces it has is "+noOfPieces+".";
            bw.append(msg);
            bw.newLine();
            bw.flush();
            bw.close();
            noOfPieces = noOfPieces;
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /*Log into file when download completed */
    public static void downloadComplete(peer p1) {
        File f = new File(Constants.LOG_PEER_PREFIX + p1.getPeerId());
        try {
            if (!f.exists()) {
                f.createNewFile();
            }
            FileWriter fw = new FileWriter(f);
            bw = new BufferedWriter(fw);
            LocalDateTime ldt = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.TIME_PATTERN);
            String time = ldt.format(formatter);
            bw.append("["+time+"]: Peer "+p1.getPeerId()+" has downloaded the complete file.");
            bw.newLine();
            bw.flush();
            bw.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public static int getNoOfPieces() {
        return noOfPieces;
    }
}

/*Class for Constants */
class Constants {
    public static final String PEERINFO_FILE = "PeerInfo.cfg";
    public static final String COMMON_FILE = "Common.cfg";
    public static final String PEER_PREFIX = "peer_";
    public static final String LOG_PEER_PREFIX = "log_peer_";
    public static final String FILE_SIZE = "FileSize";
    public static final String PIECE_SIZE = "PieceSize";
    public static final String FILE_NAME = "FileName";
    public static final String TIME_PATTERN = "dd-MM-yyyy HH:mm:ss";
}