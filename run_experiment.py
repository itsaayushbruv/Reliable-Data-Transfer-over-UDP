"""
Run real UDP-transfer experiments A-D for the college project.

Run from this folder on Windows:
    python run_experiments.py A
    python run_experiments.py B
    python run_experiments.py C
    python run_experiments.py D
    python run_experiments.py all

The runner records actual measurements. A trial is successful only if the sender
finishes, the receiver exits normally, and the received file's SHA-256 matches.
"""
import argparse
import csv
import hashlib
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "experiment_results"
INPUT_FILE = ROOT / "test.txt"
TRIAL_TIMEOUT_SECONDS = 180

PROTOCOLS = ("stop_wait", "gbn", "sr")
LOSS_RATES = (0.00, 0.10)  # Simplified Experiment A: no loss vs. 10% loss
WINDOW_SIZES = (1, 2, 4, 8, 16)
REORDER_RATES = (0.00, 0.10, 0.20, 0.30, 0.50)

# Each tuple is (label, initial RTO, minimum RTO, maximum RTO).
RTO_CONFIGS = (
    ("aggressive", 0.20, 0.05, 2.00),
    ("default",    1.00, 0.20, 5.00),
    ("conservative", 2.00, 0.50, 5.00),
)

FIELDS = [
    "experiment", "protocol", "loss_rate", "window_size",
    "reordering_probability", "rto_label", "rto_initial_s",
    "rto_min_s", "rto_max_s", "random_seed", "input_bytes",
    "elapsed_s", "goodput_bps", "data_transmissions",
    "data_retransmissions", "fin_transmissions", "fin_retransmissions",
    "channel_packets_observed", "channel_packets_dropped",
    "channel_packets_corrupted", "channel_duplicate_copies",
    "channel_packets_selected_for_reordering", "channel_packets_forwarded",
    "sender_exit_code", "receiver_exit_code", "hash_match", "success", "error",
]


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_int(pattern, text):
    match = re.search(pattern, text)
    return int(match.group(1)) if match else ""


def parse_float(pattern, text):
    match = re.search(pattern, text)
    return float(match.group(1)) if match else ""


def read_text(path):
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def stop_process(proc, log_path):
    """Stop a child and allow channel.py's KeyboardInterrupt handler to log stats."""
    if proc is None or proc.poll() is not None:
        return
    try:
        if os.name == "nt":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.send_signal(signal.SIGINT)
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.terminate()
            proc.wait(timeout=3)
        except Exception:
            try:
                proc.kill()
                proc.wait(timeout=3)
            except Exception:
                pass


def launch_logged(command, log_path, env=None):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if env is None:
        env = os.environ.copy()
        # Avoid accidentally inheriting an unrelated Python installation path.
        env.pop("PYTHONPATH", None)
        env.pop("PYTHONHOME", None)
    handle = open(log_path, "w", encoding="utf-8", errors="replace")
    kwargs = {"cwd": ROOT, "stdout": handle, "stderr": subprocess.STDOUT, "env": env}
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    try:
        proc = subprocess.Popen(command, **kwargs)
    finally:
        handle.close()
    return proc


def channel_stats(text):
    patterns = {
        "channel_packets_observed": r"Packets observed:\s*(\d+)",
        "channel_packets_dropped": r"Packets dropped:\s*(\d+)",
        "channel_packets_corrupted": r"Packets corrupted:\s*(\d+)",
        "channel_duplicate_copies": r"Duplicate copies made:\s*(\d+)",
        "channel_packets_selected_for_reordering": r"Packets reordered:\s*(\d+)",
        "channel_packets_forwarded": r"Packets forwarded:\s*(\d+)",
    }
    return {key: parse_int(pattern, text) for key, pattern in patterns.items()}


def run_trial(experiment, protocol, loss=0.0, window=4, reordering=0.0,
              rto_label="", rto_initial=1.0, rto_min=0.2, rto_max=5.0,
              seed=42):
    RESULTS.mkdir(parents=True, exist_ok=True)
    logs = RESULTS / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    tag = (f"{experiment}_{protocol}_loss{loss:.2f}_w{window}_"
           f"reorder{reordering:.2f}_{rto_label or 'normal'}_seed{seed}")
    receiver_log = logs / f"{tag}_receiver.log"
    channel_log = logs / f"{tag}_channel.log"
    sender_log = logs / f"{tag}_sender.log"
    output_file = RESULTS / f"{tag}_received.bin"

    row = {
        "experiment": experiment, "protocol": protocol, "loss_rate": loss,
        "window_size": window, "reordering_probability": reordering,
        "rto_label": rto_label, "rto_initial_s": rto_initial,
        "rto_min_s": rto_min, "rto_max_s": rto_max, "random_seed": seed,
        "input_bytes": INPUT_FILE.stat().st_size, "elapsed_s": "",
        "goodput_bps": "", "data_transmissions": "", "data_retransmissions": "",
        "fin_transmissions": "", "fin_retransmissions": "",
        "channel_packets_observed": "", "channel_packets_dropped": "",
        "channel_packets_corrupted": "", "channel_duplicate_copies": "",
        "channel_packets_selected_for_reordering": "",
        "channel_packets_forwarded": "", "sender_exit_code": "",
        "receiver_exit_code": "", "hash_match": False, "success": False, "error": "",
    }

    receiver = channel = sender = None
    start = None
    try:
        receiver_cmd = [
            sys.executable, "-u", "Receiver.py", "--protocol", protocol,
            "--output", str(output_file),
        ]
        if protocol in ("gbn", "sr"):
            receiver_cmd += ["--window", str(window), "--linger", "0.2"]
        receiver = launch_logged(receiver_cmd, receiver_log)
        time.sleep(0.25)
        if receiver.poll() is not None:
            raise RuntimeError("Receiver exited during startup; inspect its log.")

        channel_cmd = [
            sys.executable, "-u", "channel.py", "--loss", str(loss),
            "--reordering", str(reordering), "--seed", str(seed),
        ]
        channel = launch_logged(channel_cmd, channel_log)
        time.sleep(0.25)
        if channel.poll() is not None:
            raise RuntimeError("Channel exited during startup; inspect its log.")

        sender_env = os.environ.copy()
        sender_env.pop("PYTHONPATH", None)
        sender_env.pop("PYTHONHOME", None)
        sender_env["UDP_RTO_INITIAL"] = str(rto_initial)
        sender_env["UDP_RTO_MIN"] = str(rto_min)
        sender_env["UDP_RTO_MAX"] = str(rto_max)
        sender_env["UDP_RTO_ALPHA"] = "0.875"
        sender_cmd = [
            sys.executable, "-u", "Sender_app.py", str(INPUT_FILE),
            "--protocol", protocol, "--window", str(window),
        ]

        start = time.perf_counter()
        sender = launch_logged(sender_cmd, sender_log, env=sender_env)
        sender.wait(timeout=TRIAL_TIMEOUT_SECONDS)
        elapsed = time.perf_counter() - start
        row["elapsed_s"] = round(elapsed, 6)
        row["sender_exit_code"] = sender.returncode

        # Receiver flushes/closes the output file after its FIN linger period.
        if sender.returncode == 0 and "Transfer complete." in read_text(sender_log):
            receiver.wait(timeout=12)
        else:
            raise RuntimeError(
                f"Sender did not report successful completion (exit={sender.returncode})."
            )
        row["receiver_exit_code"] = receiver.returncode

        sender_text = read_text(sender_log)
        row["data_transmissions"] = parse_int(r"DATA transmissions:\s*(\d+)", sender_text)
        row["data_retransmissions"] = parse_int(r"DATA retransmissions:\s*(\d+)", sender_text)
        row["fin_transmissions"] = parse_int(r"FIN transmissions:\s*(\d+)", sender_text)
        row["fin_retransmissions"] = parse_int(r"FIN retransmissions:\s*(\d+)", sender_text)

        row.update(channel_stats(read_text(channel_log)))
        if output_file.exists():
            row["hash_match"] = sha256(INPUT_FILE) == sha256(output_file)
        row["success"] = (
            sender.returncode == 0
            and receiver.returncode == 0
            and row["hash_match"] is True
            and row["data_transmissions"] != ""
        )
        if row["success"]:
            row["goodput_bps"] = round(row["input_bytes"] * 8 / elapsed, 3)
        else:
            row["error"] = "Transfer, receiver exit, statistics, or SHA-256 verification failed."
    except subprocess.TimeoutExpired:
        row["error"] = f"Trial timed out after {TRIAL_TIMEOUT_SECONDS} seconds."
        if sender is not None:
            row["sender_exit_code"] = sender.poll()
    except Exception as exc:
        row["error"] = str(exc)
    finally:
        # Stop channel first so it can print its counters, then stop any leftovers.
        stop_process(channel, channel_log)
        for proc in (sender, receiver):
            if proc is not None and proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait(timeout=3)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
        row.update(channel_stats(read_text(channel_log)))

    print(
        f"{experiment}: {protocol}, loss={loss:.0%}, window={window}, "
        f"reorder={reordering:.0%}, success={row['success']}, "
        f"retransmissions={row['data_retransmissions']}, "
        f"goodput={row['goodput_bps']} bps"
    )
    return row


def write_csv(experiment, rows):
    RESULTS.mkdir(parents=True, exist_ok=True)
    path = RESULTS / f"experiment_{experiment}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved CSV: {path}")


def plot_results(experiment, rows):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("Plots not created: install matplotlib with `python -m pip install matplotlib`.")
        return

    RESULTS.mkdir(parents=True, exist_ok=True)
    good = [r for r in rows if r["success"] and r["goodput_bps"] != ""]
    if not good:
        print(f"No successful trials for Experiment {experiment}; no plot created.")
        return

    if experiment == "A":
        xfield, xlabel, yfield, ylabel = "loss_rate", "Packet loss probability", "goodput_bps", "Goodput (bits/s)"
        groups = PROTOCOLS
        for protocol in groups:
            subset = sorted((r for r in good if r["protocol"] == protocol), key=lambda r: r[xfield])
            plt.plot([r[xfield] * 100 for r in subset], [r[yfield] for r in subset], marker="o", label=protocol)
        plt.xlabel("Packet loss rate (%)")
        plt.ylabel(ylabel)
        plt.title("Experiment A: Goodput vs. Packet Loss")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(RESULTS / "experiment_A_goodput_vs_loss.png", dpi=160)
        plt.close()

    elif experiment == "B":
        for protocol in ("gbn", "sr"):
            subset = sorted((r for r in good if r["protocol"] == protocol), key=lambda r: r["window_size"])
            plt.plot([r["window_size"] for r in subset], [r["goodput_bps"] for r in subset], marker="o", label=protocol)
        plt.xlabel("Window size (packets)")
        plt.ylabel("Goodput (bits/s)")
        plt.title("Experiment B: Goodput vs. Window Size (0% loss)")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(RESULTS / "experiment_B_goodput_vs_window.png", dpi=160)
        plt.close()

    elif experiment == "C":
        for protocol in PROTOCOLS:
            subset = sorted((r for r in good if r["protocol"] == protocol), key=lambda r: r["reordering_probability"])
            plt.plot([r["reordering_probability"] * 100 for r in subset],
                     [r["data_retransmissions"] for r in subset], marker="o", label=protocol)
        plt.xlabel("Reordering probability (%)")
        plt.ylabel("DATA retransmissions")
        plt.title("Experiment C: Retransmissions vs. Reordering")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(RESULTS / "experiment_C_retransmissions_vs_reordering.png", dpi=160)
        plt.close()

    elif experiment == "D":
        labels = [r["rto_label"] for r in good]
        # Group by RTO label and protocol, using measured values only.
        for field, ylabel, filename, title in (
            ("goodput_bps", "Goodput (bits/s)", "experiment_D_goodput_vs_rto.png", "Goodput vs. RTO Setting"),
            ("data_retransmissions", "DATA retransmissions", "experiment_D_retransmissions_vs_rto.png", "Retransmissions vs. RTO Setting"),
            ("elapsed_s", "Transfer completion time (s)", "experiment_D_completion_time_vs_rto.png", "Completion Time vs. RTO Setting"),
        ):
            for protocol in PROTOCOLS:
                subset = [r for r in good if r["protocol"] == protocol]
                by_label = {r["rto_label"]: r for r in subset}
                ordered = [by_label[label] for label in ("aggressive", "default", "conservative") if label in by_label]
                plt.plot([r["rto_label"] for r in ordered], [r[field] for r in ordered], marker="o", label=protocol)
            plt.xlabel("RTO configuration")
            plt.ylabel(ylabel)
            plt.title(f"Experiment D: {title} (10% loss)")
            plt.legend()
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(RESULTS / filename, dpi=160)
            plt.close()
    print(f"Plots saved under: {RESULTS}")


def run_experiment(experiment):
    rows = []
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Common input file not found: {INPUT_FILE}")

    def record(**kwargs):
        row = run_trial(experiment, **kwargs)
        rows.append(row)
        # Save after every trial so completed measurements survive an interruption.
        write_csv(experiment, rows)

    # Start a fresh CSV for this experiment.
    write_csv(experiment, rows)

    if experiment == "A":
        for loss in LOSS_RATES:
            for protocol in PROTOCOLS:
                record(protocol=protocol, loss=loss, window=4, seed=42)
    elif experiment == "B":
        for window in WINDOW_SIZES:
            for protocol in ("gbn", "sr"):
                record(protocol=protocol, loss=0.0, window=window, seed=42)
    elif experiment == "C":
        for reordering in REORDER_RATES:
            for protocol in PROTOCOLS:
                record(protocol=protocol, reordering=reordering, window=4, seed=42)
    elif experiment == "D":
        for label, initial, minimum, maximum in RTO_CONFIGS:
            for protocol in PROTOCOLS:
                record(
                    protocol=protocol, loss=0.10, window=4, rto_label=label,
                    rto_initial=initial, rto_min=minimum, rto_max=maximum, seed=42,
                )
    plot_results(experiment, rows)


def main():
    parser = argparse.ArgumentParser(description="Run reliable UDP transfer experiments A-D.")
    parser.add_argument("experiment", choices=("quick", "A", "B", "C", "D", "all"),
                        help="quick=6 basic tests; A=loss, B=window, C=reordering, D=RTO tuning, or all")
    args = parser.parse_args()
    if args.experiment == "quick":
        # Six basic tests: each protocol at 0% and 10% packet loss.
        run_experiment("A")
        return
    experiments = ("A", "B", "C", "D") if args.experiment == "all" else (args.experiment,)
    for experiment in experiments:
        print(f"\n========== EXPERIMENT {experiment} ==========")
        run_experiment(experiment)


if __name__ == "__main__":
    main()
