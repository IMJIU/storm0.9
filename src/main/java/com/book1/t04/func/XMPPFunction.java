package com.book1.t04.func;

import java.util.Map;

import org.apache.log4j.Logger;
import org.jivesoftware.smack.ConnectionConfiguration;
import org.jivesoftware.smack.SmackException.NotConnectedException;
import org.jivesoftware.smack.XMPPConnection;
import org.jivesoftware.smack.packet.Message;
import org.jivesoftware.smack.packet.Message.Type;
import org.jivesoftware.smack.tcp.XMPPTCPConnection;

import com.book1.t04.EWMA;
import com.book1.t04.EWMA.Time;
import com.book1.t04.msg.MessageMapper;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class XMPPFunction extends BaseFunction{
	private static final Logger log = Logger.getLogger(XMPPFunction.class);
	
	public static final String XMPP_TO = "storm.xmpp.to";
	public static final String XMPP_USER = "storm.xmpp.user";
	public static final String XMPP_PASSWORD = "storm.xmpp.password";
	public static final String XMPP_SERVER = "storm.xmpp.server";
	
	private XMPPConnection xmppConnection ;
	private String to ;
	private MessageMapper mapper;
	
	public  XMPPFunction(MessageMapper m) {
		mapper = m;
	}
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub
		super.prepare(conf, context);
		to = (String) conf.get(XMPP_TO);
		ConnectionConfiguration config = new ConnectionConfiguration((String)conf.get(XMPP_SERVER));
		this.xmppConnection = new XMPPTCPConnection(config);
		try {
			xmppConnection.connect();
			xmppConnection.login("jsmith", "mypass");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Message msg = new Message(to,Type.normal);
		msg.setBody(mapper.toMessageBody(tuple));
		try {
			xmppConnection.sendPacket(msg);
		} catch (NotConnectedException e) {
			e.printStackTrace();
		}
	}

}
