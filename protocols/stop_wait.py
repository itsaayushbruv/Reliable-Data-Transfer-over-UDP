import socket

from packet import DATA, ACK, FIN, make_packet, parse_packet
from rto import JacobsonRTTWithKarnsModification


"""
Sender cover from here
"""
class StopAndWaitSender:

    def __init__(self, sock, receiver_address):
        self.sock = sock
        self.receiver_address = receiver_address

        # rto calculator
        self.rto = JacobsonRTTWithKarnsModification()

        # 1st seq no
        self.seq = 0

        self.retransmissions = 0
        self.packets_sent = 0

    def send_packet(self, payload):
        packet = make_packet(
            DATA,
            seq=self.seq,
            payload=payload
        )

        retransmitted = False

        while True:

            start_time = self.rto.now()

            self.sock.sendto(
                packet,
                self.receiver_address
            )

            self.packets_sent += 1

            print(
                f"Sending DATA seq={self.seq}, "
                f"RTO={self.rto.getTimeout():.3f}s"
            )

            self.sock.settimeout(
                self.rto.getTimeout()
            )

            try:

                while True:

                    raw, address = self.sock.recvfrom(65535)

                    if address != self.receiver_address:
                        continue

                    result = parse_packet(raw)

                    if result is None:
                        continue

                    ptype, seq, ack, payload_received = result

                    if ptype != ACK:
                        continue

                    if ack == self.seq:
                        if not retransmitted:

                            artt = self.rto.now() - start_time

                            self.rto.update(artt)

                            print(
                                f"ACK {ack} received, "
                                f"ARTT={artt:.3f}s"
                            )

                        else:
                            print(
                                f"ACK {ack} received after "
                                 "retransmission"
                            )

                        self.seq += 1

                        self.sock.settimeout(None)

                        return

            except socket.timeout:

                print(
                    f"TIMEOUT for seq={self.seq}"
                )


                self.rto.timeout()

                retransmitted = True

                self.retransmissions += 1


    def send_file(self, filename, chunk_size=1000):
        with open(filename, "rb") as file:

            while True:

                data = file.read(chunk_size)

                if not data:
                    break

                self.send_packet(data)

        self.send_fin()


    def send_fin(self):
        fin_packet = make_packet(
            FIN,
            seq=self.seq
        )

        while True:

            print(
                f"Sending FIN seq={self.seq}, "
                f"RTO={self.rto.getTimeout():.3f}s"
            )

            self.sock.sendto(
                fin_packet,
                self.receiver_address
            )

            self.sock.settimeout(
                self.rto.getTimeout()
            )

            try:

                while True:

                    raw, address = self.sock.recvfrom(65535)

                    if address != self.receiver_address:
                        continue

                    result = parse_packet(raw)

                    if result is None:
                        continue

                    ptype, seq, ack, payload = result

                    if ptype == ACK and ack == self.seq:

                        print("FIN acknowledged.")

                        self.sock.settimeout(None)

                        return

            except socket.timeout:

                print(
                    f"FIN timeout for seq={self.seq}. "
                     "Retransmitting FIN..."
                )

                self.rto.timeout()

                self.retransmissions += 1

