package dhsong;

import dhsong.Connection;

import redis.clients.jedis.commands.ProtocolCommand;

public class ClientConnection extends Connection{
    public ClientConnection(){
        this("localhost", 6379);
    }
    public ClientConnection(String ip){
        this(ip, 6379);
    }
    public ClientConnection(String ip, int port){
        super(ip, port);
    }

    public void sendCommand(final ProtocolCommand cmd, final byte[]... args){
        super.sendCommand(cmd, args);
    }


}
