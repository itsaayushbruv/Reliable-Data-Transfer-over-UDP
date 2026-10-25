import socket

from packet import DATA, ACK, FIN, make_packet, parse_packet
from rto import JacobsonRTTWithKarnsModification


class SelectiveRepeatSender:
    def __init__(self, sock, channel_address, window_size=4):
        if window_size < 1:
            raise ValueError("Window size must be at least 1")

        self.sock = sock
        self.channel_address = channel_address
        self.window_size = window_size
        self.rto = JacobsonRTTWithKarnsModification()
        self.packets_sent = 0       # DATA transmissions only
        self.retransmissions = 0    # DATA retransmissions only
        self.fin_transmissions = 0
        self.fin_retransmissions = 0

    def send_file(self, filename, chunk_size=1000):
        if chunk_size < 1:
            raise ValueError("Chunk size must be at least 1")

        base = 0
        next_seq = 0
        eof = False
        outstanding = {}

        with open(filename, "rb") as file:
            while not eof or outstanding:
                # Send new packets while there is room in the window.
                while not eof and next_seq < base + self.window_size:
                    payload = file.read(chunk_size)
                    if payload == b"":
                        eof = True
                        break

                    packet = make_packet(DATA, seq=next_seq, payload=payload)
                    sent_at = self.rto.now()
                    self.sock.sendto(packet, self.channel_address)

                    outstanding[next_seq] = {
                        "packet": packet,
                        "sent_at": sent_at,
                        "timeout": self.rto.getTimeout(),
                        "retransmitted": False,
                    }
                    self.packets_sent += 1
                    print(f"Sent DATA {next_seq}")
                    next_seq += 1

                if not outstanding:
                    continue

                # Each outstanding packet has its own timer.
                now = self.rto.now()
                nearest_deadline = min(
                    item["sent_at"] + item["timeout"]
                    for item in outstanding.values()
                )
                remaining = max(0.0, nearest_deadline - now)
                self.sock.settimeout(remaining)

                try:
                    raw, address = self.sock.recvfrom(65535)
                except socket.timeout:
                    raw = None
                    address = None
                except ConnectionResetError:
                    # Windows UDP sockets can report ICMP port-unreachable.
                    # Ignore this one event and let normal retransmission timers run.
                    raw = None
                    address = None

                if raw is not None:
                    if address == self.channel_address:
                        result = parse_packet(raw)
                        if result is not None:
                            ptype, seq, ack, payload = result
                            # SR ACKs identify one particular DATA sequence number.
                            if ptype == ACK and ack in outstanding:
                                entry = outstanding[ack]
                                if not entry["retransmitted"]:
                                    sample = self.rto.now() - entry["sent_at"]
                                    self.rto.update(sample)

                                del outstanding[ack]
                                print(f"ACK {ack} received")

                                # Move the left edge past all acknowledged packets.
                                while base < next_seq and base not in outstanding:
                                    base += 1

                # Retransmit only packets whose individual timers expired.
                now = self.rto.now()
                expired = [
                    seq_num for seq_num, entry in outstanding.items()
                    if now - entry["sent_at"] >= entry["timeout"]
                ]

                for seq_num in expired:
                    entry = outstanding[seq_num]
                    self.rto.timeout()
                    self.sock.sendto(entry["packet"], self.channel_address)
                    entry["sent_at"] = self.rto.now()
                    entry["timeout"] = self.rto.getTimeout()
                    entry["retransmitted"] = True
                    self.packets_sent += 1
                    self.retransmissions += 1
                    print(
                        f"Timeout for DATA {seq_num}; retransmitting only this packet. "
                        f"RTO={self.rto.getTimeout():.3f}s"
                    )

        self.sock.settimeout(None)
        self.send_fin(next_seq)

    def send_fin(self, fin_seq):
        packet = make_packet(FIN, seq=fin_seq)

        while True:
            self.sock.sendto(packet, self.channel_address)
            self.fin_transmissions += 1
            self.sock.settimeout(self.rto.getTimeout())

            try:
                while True:
                    try:
                        raw, address = self.sock.recvfrom(65535)
                    except ConnectionResetError:
                        # Windows may report a transient UDP ICMP error.
                        continue

                    if address != self.channel_address:
                        continue

                    result = parse_packet(raw)
                    if result is None:
                        continue

                    ptype, seq, ack, payload = result

                    # FIN ACK conventions can differ between implementations:
                    # some acknowledge the FIN sequence, others the next sequence.
                    if ptype == ACK and ack in (fin_seq, fin_seq + 1):
                        print(f"FIN acknowledged (ACK={ack}).")
                        self.sock.settimeout(None)
                        return

                    # Helpful diagnostic: shows what arrived while waiting for FIN ACK.
                    if ptype == ACK:
                        print(
                            f"Ignoring ACK while waiting for FIN: "
                            f"ack={ack}, expected {fin_seq} or {fin_seq + 1}"
                        )

            except socket.timeout:
                self.rto.timeout()
                self.fin_retransmissions += 1
                print(
                    f"FIN timeout; retrying FIN. "
                    f"RTO={self.rto.getTimeout():.3f}s"
                )



class SelectiveRepeatReceiver:
    def __init__(self, sock, window_size=4, linger_timeout=6.0, verbose=True):
        if window_size < 1:
            raise ValueError("Window size must be at least 1")

        self.sock = sock
        self.window_size = window_size
        self.expected_seq = 0
        self.buffer = {}
        self.packets_received = 0
        self.out_of_order_packets = 0
        self.duplicates = 0
        self.linger_timeout = linger_timeout
        self.verbose = verbose

    def send_ack(self, seq_num, destination):
        # In Selective Repeat, ACK carries the exact packet sequence number.
        self.sock.sendto(make_packet(ACK, ack=seq_num), destination)

    def receive_file(self, output_filename):
        with open(output_filename, "wb") as file:
            while True:
                try:
                    raw, sender_address = self.sock.recvfrom(65535)
                except ConnectionResetError:
                    # Ignore transient Windows UDP ICMP errors.
                    continue
                result = parse_packet(raw)
                if result is None:
                    if self.verbose:
                        print("Invalid/corrupted packet ignored.")
                    continue

                ptype, seq, ack, payload = result

                if ptype == DATA:
                    # Accept packets in the current receive window.
                    if self.expected_seq <= seq < self.expected_seq + self.window_size:
                        if seq not in self.buffer:
                            self.buffer[seq] = payload
                            if seq != self.expected_seq:
                                self.out_of_order_packets += 1
                            if self.verbose:
                                print(f"Buffered DATA {seq}")
                        else:
                            self.duplicates += 1

                        # ACK every valid packet, including duplicates.
                        self.send_ack(seq, sender_address)

                        # Write only consecutive packets, preserving file order.
                        while self.expected_seq in self.buffer:
                            file.write(self.buffer.pop(self.expected_seq))
                            if self.verbose:
                                print(f"Wrote DATA {self.expected_seq}")
                            self.expected_seq += 1
                            self.packets_received += 1

                    elif seq < self.expected_seq:
                        # It was already written; ACK again in case its ACK was lost.
                        self.duplicates += 1
                        self.send_ack(seq, sender_address)

                    else:
                        # Too far ahead of the receive window: do not buffer it.
                        if self.verbose:
                            print(f"DATA {seq} outside receive window; ignored")

                elif ptype == FIN:
                    if seq == self.expected_seq:
                        # FIN consumes one sequence number, just like in GBN.
                        self.send_ack(seq + 1, sender_address)
                        if self.verbose:
                            print("FIN accepted.")
                        self.linger_for_fin(sender_address, seq)
                        return
                    elif self.verbose:
                        print(
                            f"FIN {seq} arrived, but next expected DATA/FIN is "
                            f"{self.expected_seq}; ignoring it."
                        )

    def linger_for_fin(self, sender_address, fin_seq):
        self.sock.settimeout(self.linger_timeout)
        try:
            while True:
                try:
                    raw, address = self.sock.recvfrom(65535)
                except socket.timeout:
                    return
                except ConnectionResetError:
                    continue

                if address != sender_address:
                    continue

                result = parse_packet(raw)
                if result is None:
                    continue

                ptype, seq, ack, payload = result
                if ptype == FIN and seq == fin_seq:
                    self.send_ack(fin_seq + 1, sender_address)
                    if self.verbose:
                        print("Duplicate FIN: ACK sent again.")
        finally:
            self.sock.settimeout(None)