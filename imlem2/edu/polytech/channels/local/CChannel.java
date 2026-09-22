package edu.polytech.channels.local;

import edu.polytech.channels.Channel;
import edu.polytech.utils.CircularBuffer;

public class CChannel implements Channel{

	private CChannel twin;
	private CircularBuffer in;
	private CircularBuffer out;
	private volatile boolean closed;

	public CChannel(int capacity) {
		if (capacity <= 0)
			throw new IllegalArgumentException();
		in = new CircularBuffer(capacity);
	}

	public CChannel(CChannel twin, int capacity) {
		this(capacity);
		this.twin = twin;
		twin.twin = this;
		twin.out = in;
		out = twin.in;
	}

	public int read(byte[] bytes, int offset, int length) {
		if (offset < 0 || length < 0 || offset + length > bytes.length)
			throw new IllegalArgumentException();
		synchronized (in) {
			while (in.empty()) {
				if (closed || twin.closed)
					return 0;
				try {
					in.wait();
				} catch (InterruptedException e) {
					return 0;
				}
			}
			if (closed)
				return 0;
			int n = 0;
			while (n < length && !in.empty())
				bytes[offset + n++] = in.pull();
			in.notifyAll();
			return n;
		}
	}

	public int write(byte[] bytes, int offset, int length) {
		if (offset < 0 || length < 0 || offset + length > bytes.length)
			throw new IllegalArgumentException();
		synchronized (out) {
			while (out.full()) {
				if (closed || twin.closed)
					return length;
				try {
					out.wait();
				} catch (InterruptedException e) {
					return length;
				}
			}
			if (closed || twin.closed)
				return length;
			int n = 0;
			while (n < length && !out.full())
				out.push(bytes[offset + n++]);
			out.notifyAll();
			return n;
		}
	}

	public void disconnect() {
		synchronized (in) {
			if (closed) {
				return;
			}
			closed = true;
			in.notifyAll();
		}
		synchronized (out) {
			out.notifyAll();
		}
	}

	public boolean disconnected() {
		if (closed) {
			return true;
		}
		synchronized (in) {
			return twin.closed && in.empty();
		}
	}
}
