import argparse
import socket

from stop_wait import StopAndWaitSender
from gbn import GoBackNSender
from sr import SelectiveRepeatSender


def main():
    parser = argparse.ArgumentParser(description="Reliable UDP file sender")
    parser.add_argument("input_file")
    parser.add_argument(
        "--protocol",
        choices=["stop_wait", "gbn", "sr"],
        default="stop_wait",
    )
    parser.add_argument("--window", type=int, default=4)
    args = parser.parse_args()

    if args.window < 1:
        parser.error("--window must be at least 1")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    channel_address = ("127.0.0.1", 9001)

    try:
        if args.protocol == "stop_wait":
            sender = StopAndWaitSender(sock, channel_address)
        elif args.protocol == "gbn":
            sender = GoBackNSender(sock, channel_address, window_size=args.window)
        else:
            sender = SelectiveRepeatSender(sock, channel_address, window_size=args.window)

        print(f"Protocol: {args.protocol}")
        print(f"Window size: {args.window}")
        print(f"Input file: {args.input_file}")
        sender.send_file(args.input_file)

        print("Transfer complete.")
        print(f"DATA transmissions: {sender.packets_sent}")
        print(f"DATA retransmissions: {sender.retransmissions}")
        print(f"FIN transmissions: {sender.fin_transmissions}")
        print(f"FIN retransmissions: {sender.fin_retransmissions}")
        print(f"Final RTO: {sender.rto.getTimeout():.3f}s")
    finally:
        sock.close()


if __name__ == "__main__":
    main()