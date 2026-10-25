import argparse
import heapq
import json
import os
import random
import socket
import time

from packet import parse_packet, DATA, ACK, FIN

class UnreliableChannel:
    """
    UDP proxy that can simulate loss, corruption, duplication,
    reordering, delay, and jitter in both directions.

    One sender and one receiver are supported at a time.
    """

    def __init__(
        self,
        listen_address,
        receiver_address,
        loss=0.0,
        corruption=0.0,
        duplication=0.0,
        reordering=0.0,
        delay=0.0,
        jitter=0.0,
        seed=None,
        stats_file=None,
    ):
        self.listen_address = listen_address
        self.receiver_address = receiver_address

        self.loss = loss
        self.corruption = corruption
        self.duplication = duplication
        self.reordering = reordering
        self.delay = delay          # milliseconds
        self.jitter = jitter        # milliseconds

        self.random = random.Random(seed)

        for name in ("loss", "corruption", "duplication", "reordering"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")

        if self.delay < 0 or self.jitter < 0:
            raise ValueError("delay and jitter cannot be negative")

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(self.listen_address)

        self.sender_address = None
        self.pending = []
        self.counter = 0
        self.stats_file = stats_file

        self.total_packets = 0
        self.dropped_packets = 0
        self.corrupted_packets = 0
        self.duplicated_packets = 0
        self.reordered_packets = 0
        self.forwarded_packets = 0

        # Reordering holds one selected packet briefly so a later packet
        # can overtake it. Held packets are always eventually scheduled.
        self.reorder_hold = []

    def get_statistics(self):
        return {
            "packets_observed": self.total_packets,
            "packets_dropped": self.dropped_packets,
            "packets_corrupted": self.corrupted_packets,
            "duplicate_copies": self.duplicated_packets,
            "packets_reordered": self.reordered_packets,
            "packets_forwarded": self.forwarded_packets,
        }

    def write_statistics(self):
        """Persist counters during execution, so forced shutdown doesn't lose them."""
        if not self.stats_file:
            return
        path = os.path.abspath(self.stats_file)
        temp_path = path + ".tmp"
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(self.get_statistics(), f)
            os.replace(temp_path, path)
        except OSError as exc:
            print(f"CHANNEL: could not write statistics file: {exc}", flush=True)

    def get_delay(self):
        """Return base delay plus random jitter, in seconds."""
        jitter_ms = 0.0
        if self.jitter:
            jitter_ms = self.random.uniform(-self.jitter, self.jitter)

        delay_ms = max(0.0, self.delay + jitter_ms)
        return delay_ms / 1000.0

    def schedule_packet(self, packet, destination, extra_delay=0.0):
        delivery_time = time.monotonic() + self.get_delay() + extra_delay
        self.counter += 1
        heapq.heappush(
            self.pending,
            (delivery_time, self.counter, destination, packet),
        )

    def corrupt_packet(self, packet):
        """Flip one bit in a random byte of a non-empty packet."""
        if not packet:
            return packet

        changed = bytearray(packet)
        position = self.random.randrange(len(changed))
        changed[position] ^= 1
        return bytes(changed)

    def process_packet(self, packet, destination):
        self.total_packets += 1

        # 1. Drop the packet completely.
        if self.random.random() < self.loss:
            self.dropped_packets += 1
            print("CHANNEL: packet LOST")
            return

        # 2. Corrupt the packet before forwarding.
        if self.random.random() < self.corruption:
            packet = self.corrupt_packet(packet)
            self.corrupted_packets += 1
            print("CHANNEL: packet CORRUPTED")

        # 3. Realistic reordering: hold a selected packet briefly.
        # A later packet can then be delivered first. If no later packet
        # arrives, the held packet is still released after the hold time.
        if self.random.random() < self.reordering:
            hold_seconds = 0.050
            self.reorder_hold.append(
                (time.monotonic() + hold_seconds, packet, destination)
            )
            self.reordered_packets += 1
            print("CHANNEL: packet held for REORDERING")
        else:
            self.schedule_packet(packet, destination)

        # 4. Optionally send a duplicate as well.
        if self.random.random() < self.duplication:
            self.duplicated_packets += 1
            print("CHANNEL: packet DUPLICATED")
            self.schedule_packet(packet, destination)
        self.write_statistics()
    def release_held_packets(self):
        """Release reordered packets once their hold period expires."""
        now = time.monotonic()
        still_held = []

        for release_at, packet, destination in self.reorder_hold:
            if release_at <= now:
                self.schedule_packet(packet, destination)
            else:
                still_held.append((release_at, packet, destination))

        self.reorder_hold = still_held

    def deliver_ready_packets(self):
        self.release_held_packets()
        now = time.monotonic()

        while self.pending and self.pending[0][0] <= now:
            _, _, destination, packet = heapq.heappop(self.pending)
            self.sock.sendto(packet, destination)
            self.forwarded_packets += 1
            self.write_statistics()

    def run(self):
        print("Unreliable UDP channel started.")
        print(f"Listening on: {self.listen_address}")
        print(f"Receiver:     {self.receiver_address}")
        print("Press Ctrl+C to stop.")

        while True:
            self.deliver_ready_packets()

            # Keep the wait short so scheduled packets are delivered promptly.
            self.sock.settimeout(0.005)

            try:
                packet, address = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            except ConnectionResetError:
                # Windows may surface an ICMP "port unreachable" for UDP.
                # Treat it as a transient network event, not a fatal crash.
                continue

            if address == self.receiver_address:
                # Receiver -> sender (ACKs and other control packets).
                if self.sender_address is None:
                    print("CHANNEL: receiver packet arrived before sender was known; ignored")
                    continue
                destination = self.sender_address
            else:
                # Sender -> receiver. Update the sender address for each
                # new source port, allowing the sender app to be restarted.
                if self.sender_address != address:
                    self.sender_address = address
                    print(f"Sender discovered/updated: {self.sender_address}")
                destination = self.receiver_address

            result = parse_packet(packet)
            if result is not None:
                ptype, seq, ack, payload = result

                if ptype == ACK:
                    print(
                        f"CHANNEL: ACK received from {address}, "
                        f"ack={ack}, forwarding to {destination}"
                    )
                elif ptype == DATA:
                    print(f"CHANNEL: DATA seq={seq}, forwarding to {destination}")
                elif ptype == FIN:
                    print(f"CHANNEL: FIN seq={seq}, forwarding to {destination}")

            self.process_packet(packet, destination)


def main():
    parser = argparse.ArgumentParser(
        description="Unreliable UDP channel emulator"
    )
    parser.add_argument("--loss", type=float, default=0.0,
                        help="Packet loss probability (0 to 1)")
    parser.add_argument("--corruption", type=float, default=0.0,
                        help="Packet corruption probability (0 to 1)")
    parser.add_argument("--duplication", type=float, default=0.0,
                        help="Packet duplication probability (0 to 1)")
    parser.add_argument("--reordering", type=float, default=0.0,
                        help="Packet reordering probability (0 to 1)")
    parser.add_argument("--delay", type=float, default=0.0,
                        help="Base delay in milliseconds")
    parser.add_argument("--jitter", type=float, default=0.0,
                        help="Jitter in milliseconds")
    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed for repeatable impairment choices")
    parser.add_argument("--stats-file", default=None,
                        help="Optional JSON file updated with channel counters")
    args = parser.parse_args()

    channel = UnreliableChannel(
        listen_address=("127.0.0.1", 9001),
        receiver_address=("127.0.0.1", 9000),
        loss=args.loss,
        corruption=args.corruption,
        duplication=args.duplication,
        reordering=args.reordering,
        delay=args.delay,
        jitter=args.jitter,
        seed=args.seed,
        stats_file=args.stats_file,
    )

    channel.write_statistics()
    try:
        channel.run()
    except KeyboardInterrupt:
        channel.write_statistics()
        print("\nChannel stopped.")
        print("\nChannel statistics:")
        print(f"Packets observed:     {channel.total_packets}")
        print(f"Packets dropped:      {channel.dropped_packets}")
        print(f"Packets corrupted:    {channel.corrupted_packets}")
        print(f"Duplicate copies made:{channel.duplicated_packets}")
        print(f"Packets reordered:    {channel.reordered_packets}")
        print(f"Packets forwarded:    {channel.forwarded_packets}")
    finally:
        channel.sock.close()


if __name__ == "__main__":
    main()
