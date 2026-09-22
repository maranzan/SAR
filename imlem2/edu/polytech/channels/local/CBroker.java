package edu.polytech.channels.local;

import java.util.HashMap;

import edu.polytech.channels.Broker;
import edu.polytech.channels.Channel;

public class CBroker implements Broker {

	private static final int cap = 64;
	private final HashMap<Integer, Rdv> accepts = new HashMap<Integer, Rdv>();

	private final String name;
	


	CBroker(String name) {
		if (name == null)
			throw new IllegalArgumentException("nom nul");
		this.name = name;
		BrokerManager.add(this);
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Channel connect(String name, int port) {
		if (port < 0)
			throw new IllegalArgumentException("port invalide : " + port);

		CBroker remote = BrokerManager.get(name);
		if (remote == null)
			return null;

		synchronized (remote) {
			Rdv rdv = remote.accepts.get(port);
			while (rdv == null || rdv.taken) {
				try {
					remote.wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException("connect interrompu");
				}
				rdv = remote.accepts.get(port);
			}

			rdv.taken = true;
			remote.accepts.remove(port);

			CChannel acceptSide = new CChannel(cap);
			CChannel connectSide = new CChannel(acceptSide, cap);

			rdv.acceptSide = acceptSide;
			remote.notifyAll();

			return (Channel) connectSide;
		}
	}

	@Override
	public Channel accept(int port) {
		if (port < 0)
			throw new IllegalArgumentException("port invalide : " + port);

		Rdv rdv = new Rdv();
		synchronized (this) {
			if (accepts.containsKey(port))
				throw new IllegalStateException("accept deja en attente sur le port " + port);
			accepts.put(port, rdv);
			notifyAll();

			while (!rdv.taken) {
				try {
					wait();
				} catch (InterruptedException e) {
					accepts.remove(port);
					Thread.currentThread().interrupt();
					throw new IllegalStateException("accept interrompu");
				}
			}
			return (Channel) rdv.acceptSide;
		}
	}
	
	private static class Rdv {
		boolean taken = false;
		CChannel acceptSide;
	}

}
