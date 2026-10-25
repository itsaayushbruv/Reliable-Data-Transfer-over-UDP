import socket

from packet import DATA, ACK, FIN, make_packet, parse_packet
from rto import JacobsonRTTWithKarnsModification


class GoBackNSender:

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

        # Store packets that have been sent but not acknowledged.
        outstanding = {}

        # Timer for the oldest unacknowledged packet.
        base_timer_started_at = None

        with open(filename, "rb") as file:

            while not eof or base < next_seq:

                # Fill the sliding window.
                while not eof and next_seq < base + self.window_size:

                    payload = file.read(chunk_size)

                    if payload == b"":
                        eof = True
                        break

                    packet = make_packet(
                        DATA,
                        seq=next_seq,
                        payload=payload
                    )

                    sent_at = self.rto.now()

                    # Window was empty: this becomes the base packet.
                    if base == next_seq:
                        base_timer_started_at = sent_at

                    self.sock.sendto(
                        packet,
                        self.channel_address
                    )

                    self.packets_sent += 1

                    outstanding[next_seq] = {
                        "packet": packet,
                        "sent_at": sent_at,
                        "retransmitted": False
                    }

                    print(f"Sent DATA {next_seq}")

                    next_seq += 1

                # No outstanding packets remain.
                if base == next_seq:
                    continue

                timed_out = False

                # Wait for a cumulative ACK.
                while True:

                    elapsed = (
                        self.rto.now() - base_timer_started_at
                    )

                    remaining = self.rto.getTimeout() - elapsed

                    if remaining <= 0:
                        timed_out = True
                        break

                    self.sock.settimeout(remaining)

                    try:
                        raw, address = self.sock.recvfrom(65535)

                    except socket.timeout:
                        timed_out = True
                        break

                    # Ignore packets from an unexpected address.
                    if address != self.channel_address:
                        continue

                    result = parse_packet(raw)

                    if result is None:
                        continue

                    ptype, seq, ack, payload = result

                    if ptype != ACK:
                        continue

                    # In GBN, ACK means:
                    # "I expect this sequence number next."
                    #
                    # ACK 3 means DATA 0, 1 and 2
                    # have been received in order.

                    if ack <= base or ack > next_seq:
                        continue

                    # Take an RTT sample only if:
                    # 1. ACK advances base by exactly one.
                    # 2. That packet was never retransmitted.
                    #
                    # This avoids ambiguous RTT measurements.
                    if ack == base + 1:

                        entry = outstanding[base]

                        if not entry["retransmitted"]:

                            artt = (
                                self.rto.now() - entry["sent_at"]
                            )

                            self.rto.update(artt)

                    # Remove all cumulatively acknowledged packets.
                    for seq_num in range(base, ack):
                        outstanding.pop(seq_num, None)

                    base = ack

                    # Restart the timer for the new base.
                    if base < next_seq:
                        base_timer_started_at = self.rto.now()
                    else:
                        base_timer_started_at = None

                    self.sock.settimeout(None)
                    break

                if timed_out:

                    # Timeout: apply Karn's exponential backoff.
                    self.rto.timeout()

                    retransmit_at = self.rto.now()
                    base_timer_started_at = retransmit_at

                    print(
                        f"Timeout at base={base}. "
                        f"Retransmitting outstanding packets. "
                        f"New RTO={self.rto.getTimeout():.3f}s"
                    )

                    # GBN retransmits EVERY outstanding packet.
                    for seq_num in range(base, next_seq):

                        entry = outstanding[seq_num]

                        self.sock.sendto(
                            entry["packet"],
                            self.channel_address
                        )

                        self.packets_sent += 1
                        self.retransmissions += 1

                        entry["sent_at"] = retransmit_at
                        entry["retransmitted"] = True

        self.sock.settimeout(None)

        # All DATA packets are acknowledged. Send FIN.
        self.send_fin(next_seq)

    def send_fin(self, fin_seq):

        packet = make_packet(FIN, seq=fin_seq)

        while True:

            sent_at = self.rto.now()
            self.sock.sendto(packet, self.channel_address)
            self.fin_transmissions += 1

            deadline = sent_at + self.rto.getTimeout()
            timed_out = False

            while True:

                remaining = deadline - self.rto.now()

                if remaining <= 0:
                    timed_out = True
                    break

                self.sock.settimeout(remaining)

                try:
                    raw, address = self.sock.recvfrom(65535)

                except socket.timeout:
                    timed_out = True
                    break

                if address != self.channel_address:
                    continue

                result = parse_packet(raw)

                if result is None:
                    continue

                ptype, seq, ack, payload = result

                # FIN uses the same cumulative ACK convention.
                if ptype == ACK and ack == fin_seq + 1:
                    self.sock.settimeout(None)
                    print("FIN acknowledged.")
                    return

            if timed_out:
                self.rto.timeout()
                self.fin_retransmissions += 1

                print(
                    "FIN timeout. Retrying FIN. "
                    f"New RTO={self.rto.getTimeout():.3f}s"
                )


class GoBackNReceiver:

    def __init__(self, sock, linger_timeout=6.0, verbose=True):

        self.sock = sock
        self.expected_seq = 0

        self.packets_received = 0
        self.out_of_order_packets = 0
        self.duplicates = 0

        self.linger_timeout = linger_timeout
        self.verbose = verbose

    def send_ack(self, destination):

        # Cumulative ACK = next sequence number expected.
        ack_packet = make_packet(
            ACK,
            ack=self.expected_seq
        )

        self.sock.sendto(ack_packet, destination)

    def receive_file(self, output_filename):

        with open(output_filename, "wb") as file:

            while True:

                raw, sender_address = self.sock.recvfrom(65535)

                result = parse_packet(raw)

                # Ignore corrupted or invalid packets.
                if result is None:
                    continue

                ptype, seq, ack, payload = result

                if ptype == DATA:

                    if seq == self.expected_seq:

                        # Correct in-order packet: write it once.
                        file.write(payload)

                        self.expected_seq += 1
                        self.packets_received += 1

                        if self.verbose:
                            print(
                                f"Accepted DATA {seq}; "
                                f"next expected={self.expected_seq}"
                            )

                    else:

                        # GBN discards both out-of-order data
                        # and duplicate data.
                        if seq > self.expected_seq:
                            self.out_of_order_packets += 1
                        else:
                            self.duplicates += 1

                        if self.verbose:
                            print(
                                f"Discarded DATA {seq}; "
                                f"expected={self.expected_seq}"
                            )

                    # ACK the current cumulative position,
                    # even if this packet was discarded.
                    self.send_ack(sender_address)

                elif ptype == FIN:

                    if seq == self.expected_seq:

                        # FIN occupies one sequence number.
                        self.expected_seq = seq + 1

                        self.send_ack(sender_address)

                        if self.verbose:
                            print("FIN accepted.")

                        # Stay alive in case the FIN ACK is lost.
                        self.linger_for_fin(
                            sender_address,
                            seq
                        )

                        return

                    else:
                        # Report the first sequence still expected.
                        self.send_ack(sender_address)

    def linger_for_fin(self, sender_address, fin_seq):

        # This should exceed the sender's maximum RTO plus
        # the ordinary channel delay.
        self.sock.settimeout(self.linger_timeout)

        try:

            while True:

                try:
                    raw, address = self.sock.recvfrom(65535)

                except socket.timeout:
                    return

                if address != sender_address:
                    continue

                result = parse_packet(raw)

                if result is None:
                    continue

                ptype, seq, ack, payload = result

                if ptype == FIN and seq == fin_seq:

                    # The previous FIN ACK may have been lost.
                    self.send_ack(sender_address)

                    if self.verbose:
                        print("Duplicate FIN: ACK sent again.")

                elif ptype == DATA:

                    # Late or duplicate DATA: don't write it again.
                    self.send_ack(sender_address)

        finally:
            self.sock.settimeout(None)