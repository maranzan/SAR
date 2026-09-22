package edu.polytech.channels.local;

import java.util.HashMap;

public class BrokerManager {
	public static HashMap<String, CBroker> brokerList;

	public BrokerManager() {
		brokerList = new HashMap<String, CBroker>();
	}

	public static synchronized void add(CBroker broker) {
		if (brokerList.containsKey(broker.getName())) {
			return;
		}
		brokerList.put(broker.getName(), broker);
	}

	public static synchronized void remove(CBroker broker) {
		brokerList.remove(broker.getName());
	}

	public static synchronized CBroker get(String name) {
		return brokerList.get(name);
	}
}
