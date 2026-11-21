#!/usr/bin/env python3
"""模拟串口设备：周期性输出 PKT 包，并响应简单命令，可通过 lrzsz 做 ZMODEM 收发。

用法::

    python tools/mock_device.py --device PATH [--baud 115200] [--name dev]
        [--interval 0.5] [--files-dir DIR] [--recv-dir DIR]

PKT 行格式（CRLF 结尾）::

    PKT name=<name> seq=<n> t=<epoch_ms> payload=<16 hex> crc=<4 hex>

crc 为 payload 原始 8 字节的 CRC16-CCITT（poly 0x1021，init 0xFFFF，即 CCITT-FALSE）。
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import threading
import time

import serial


def crc16_ccitt(data: bytes) -> int:
    """CRC16-CCITT（poly 0x1021, init 0xFFFF）。"""
    crc = 0xFFFF
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def make_packet(name: str, seq: int, payload: bytes) -> bytes:
    t = int(time.time() * 1000)
    return (f"PKT name={name} seq={seq} t={t} payload={payload.hex()} "
            f"crc={crc16_ccitt(payload):04X}\r\n").encode()


def _find(*names: str) -> str | None:
    for n in names:
        p = shutil.which(n)
        if p:
            return p
    return None


class MockDevice:
    def __init__(self, ser: serial.Serial, name: str, interval: float,
                 files_dir: str, recv_dir: str) -> None:
        self.ser = ser
        self.name = name
        self.interval = interval
        self.files_dir = files_dir
        self.recv_dir = recv_dir
        self.lock = threading.Lock()      # 保护所有写
        self.stream = threading.Event()   # 置位 = 允许周期包
        self.stream.set()
        self.stop = threading.Event()
        self.seq = 0

    def write_line(self, text: str) -> None:
        with self.lock:
            self.ser.write(text.encode() + b"\r\n")

    def _packets(self) -> None:
        while not self.stop.is_set():
            if self.interval <= 0 or not self.stream.is_set():
                self.stop.wait(0.05)
                continue
            with self.lock:
                # 持锁再检查一次，避免与 rz/sz 抢占串口
                if self.stream.is_set():
                    self.seq += 1
                    self.ser.write(make_packet(self.name, self.seq, os.urandom(8)))
            self.stop.wait(self.interval)

    def _transfer(self, tool: tuple[str, ...], args: list[str], cwd: str, label: str) -> None:
        exe = _find(*tool)
        if not exe:
            self.write_line(f"ERR {label}: lrzsz not found")
            return
        os.makedirs(cwd, exist_ok=True)
        with self.lock:                    # 传输期间持锁：父进程不读不写串口
            was_streaming = self.stream.is_set()
            self.stream.clear()
            fd = self.ser.fileno()
            # pyserial 以 O_NONBLOCK 打开；子进程共享同一文件描述，必须切成阻塞，
            # 否则 lrz/lsz 的 write 会在 PTY 缓冲写满时 EAGAIN/短写。
            was_blocking = os.get_blocking(fd)
            os.set_blocking(fd, True)
            try:
                cmd = [exe, *args] if label == "sz" else [exe, "-y"]
                rc = subprocess.call(cmd, stdin=fd, stdout=fd,
                                     stderr=subprocess.DEVNULL, cwd=cwd)
            finally:
                os.set_blocking(fd, was_blocking)
                if was_streaming:          # 恢复传输前的 stream 状态
                    self.stream.set()
            self.ser.write(f"{label} done rc={rc}\r\n".encode())
            # 同时打到 stdout：中止后这行可能落在宿主的排空窗口里被丢弃，测试据此确认退出码
            print(f"{label} done rc={rc}", flush=True)

    def handle(self, line: str) -> None:
        cmd, _, rest = line.strip().partition(" ")
        if cmd == "ping":
            self.write_line("pong")
        elif cmd == "echo":
            self.write_line(rest)
        elif cmd == "stream":
            if rest.strip() == "on":
                self.stream.set()
            elif rest.strip() == "off":
                self.stream.clear()
            self.write_line(f"stream {rest.strip()}")
        elif cmd == "ls":
            os.makedirs(self.recv_dir, exist_ok=True)
            for f in sorted(os.listdir(self.recv_dir)):
                self.write_line(f)
        elif cmd == "rz":
            self._transfer(("lrz", "rz"), [], self.recv_dir, "rz")
        elif cmd == "sz":
            self._transfer(("lsz", "sz"), rest.split(), self.files_dir, "sz")
        else:
            self.write_line(f"ERR unknown: {line.strip()}")

    def run(self) -> None:
        threading.Thread(target=self._packets, daemon=True).start()
        buf = b""
        try:
            while not self.stop.is_set():
                # 传输期间 handle 持锁在命令线程内（本线程），因此这里不会并发读
                chunk = self.ser.read(256)
                if not chunk:
                    continue
                buf += chunk
                while True:
                    idx = min((i for i in (buf.find(b"\r"), buf.find(b"\n")) if i >= 0),
                              default=-1)
                    if idx < 0:
                        break
                    raw, buf = buf[:idx], buf[idx + 1:]
                    if raw.strip():
                        self.handle(raw.decode("utf-8", "replace"))
        finally:
            self.stop.set()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--device", required=True)
    ap.add_argument("--baud", type=int, default=115200)
    ap.add_argument("--name", default="dev")
    ap.add_argument("--interval", type=float, default=0.5)
    ap.add_argument("--files-dir", default=".")
    ap.add_argument("--recv-dir", default=".")
    a = ap.parse_args(argv)
    ser = serial.Serial(a.device, a.baud, timeout=0.05)
    try:
        MockDevice(ser, a.name, a.interval, a.files_dir, a.recv_dir).run()
    except KeyboardInterrupt:
        pass
    finally:
        ser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
