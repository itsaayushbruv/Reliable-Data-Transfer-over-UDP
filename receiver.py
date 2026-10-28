import argparse
import socket

from stop_wait import StopAndWaitReceiver
from gbn import GoBackNReceiver
from sr import SelectiveRepeatReceiver


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--protocol",
        choices=["stop_wait", "gbn", "sr"],
        default="stop_wait",
    )
    parser.add_argument("--output", default="output_received.txt")
    parser.add_argument("--window", type=int, default=4)
    parser.add_argument(
        "--linger",
        type=float,
        default=6.0,
    )
    args = parser.parse_args()

    if args.window < 1:
        parser.error("--window must be at least 1")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", 9000))

    print("Receiver listening on UDP port 9000")
    print(f"Protocol: {args.protocol}")

    try:
        if args.protocol == "stop_wait":
            receiver = StopAndWaitReceiver(sock)
        elif args.protocol == "gbn":
            receiver = GoBackNReceiver(sock, linger_timeout=args.linger)
        else:
            receiver = SelectiveRepeatReceiver(
                sock,
                window_size=args.window,
                linger_timeout=args.linger,
            )

        receiver.receive_file(args.output)
        print(f"File received: {args.output}")
        print(f"DATA packets accepted: {receiver.packets_received}")

        if args.protocol in ("gbn", "sr"):
            print("Out-of-order packets:", receiver.out_of_order_packets)
            print("Duplicate packets:", receiver.duplicates)
    finally:
        sock.close()


if __name__ == "__main__":
    main()