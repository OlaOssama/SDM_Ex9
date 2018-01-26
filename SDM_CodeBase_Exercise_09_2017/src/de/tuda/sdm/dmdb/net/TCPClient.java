package de.tuda.sdm.dmdb.net;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;

import javax.net.ssl.HostnameVerifier;

import de.tuda.sdm.dmdb.storage.AbstractRecord;

/**
 * Implementation of a Client component based on TCP.
 * This class is used in the send operator to send records to other peers in a parallel setup.
 * The TCPClient uses an outgoing object stream to send (serialized) objects to the server side
 * 
 * @author melhindi
 *
 */
public class TCPClient {

	protected Socket socket = null; // The client socket
	protected ObjectOutputStream objectOutputStream = null; // Stream to write to server
	
	private String host_;
	private int port_;
	/**
	 * Constructor of TCPClient
	 * @param host - The remote host (IP/domain name) to which to connect
	 * @param port - The port of the remote host to which to connect
	 * @throws UnknownHostException - Thrown when IP/domain name could not be resolved
	 * @throws IOException - Thrown on socket issues
	 */
	public TCPClient(String host, int port) throws UnknownHostException, IOException {
		host_ = host;
		port_ = port;
		
		int maxRetries = 3;
		int waitingTime = 10; //seconds
		int retryCounter = 0;
		boolean connectionSuccess = false; // indicate success to avoid retry
		while (! connectionSuccess && retryCounter < maxRetries) {
			try {
				System.out.println("TCPClient: Connecting to " + host +" on " + port);
				this.socket = new Socket(host, port);
				connectionSuccess = true;
			} catch (ConnectException e) {
				System.err.println("TCPClient: The following error occured: " + e.getMessage());
				// wait and retry...
				if (retryCounter < maxRetries) {
					++retryCounter;
					System.out.println("TCPClient: Connection not successful, will retry to connect to " + host + " on port " + port);
					try {
						System.out.println("TCPClient: Waiting for " + waitingTime + " seconds");
						Thread.sleep(waitingTime*1000);
					} catch (InterruptedException e1) {
						System.err.println("TCPClient: Got the following error:");
						e1.printStackTrace();
					}
					continue; // retry
				}
			}
		}

		this.objectOutputStream = new ObjectOutputStream(this.socket.getOutputStream());
		System.out.println("TCPClient: created ObjectOutputStream");
	}

	/**
	 * Sends a record to the a receive server through an object output stream
	 * @param record - Record to transfer to server
	 * @return - Returns true on success, else false
	 */
	public boolean sendRecord(AbstractRecord record) {
//		System.out.println("TCPClient: sending record to server");
		try {
			this.objectOutputStream.writeObject(record);
		}catch (SocketException e) {
			e.printStackTrace();
			System.exit(1);
			return false;
		}
		catch (IOException e) {
			e.printStackTrace();
			return false;
		}
//		System.out.println("TCPClient: Finished sending to server");
		return true;

	}

	/**
	 * Close connection to the remote server
	 * @return - Returns true on success, else false
	 */
	public boolean close() {
		System.out.println("TCPClient: Closing connection to " + this.socket.getRemoteSocketAddress());
		try {
			this.socket.close();
		} catch (IOException e) {
			System.err.println("Could not close Socket to " + this.socket.getRemoteSocketAddress());
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Getter method for object output stream member
	 * @return - Returns the object output stream member
	 */
	public ObjectOutputStream getObjectOutputStream() {
		return objectOutputStream;
	}


	/**
	 * Setter method for object output stream member
	 * @param objectOutputStream - ObjectOutputStrem to use for record transfer
	 */
	public void setObjectOutputStream(ObjectOutputStream objectOutputStream) {
		this.objectOutputStream = objectOutputStream;
	}
	
	public String toString() {
		return "name " + host_ + " port " + port_;
	}
}