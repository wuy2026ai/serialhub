#!/usr/bin/env python3
"""模拟串口设备：周期性输出 PKT 包，并响应简单命令，可通过 lrzsz 做 ZMODEM 收发。

用法::

    python tools/mock_device.py --device PATH [--baud 115200] [--name dev]
        [--interval 0.5] [--files-dir DIR] [--recv-dir DIR] [--rz-kill-after SECONDS]

命令：``ping`` / ``echo TEXT`` / ``stream on|off`` / ``ls`` / ``rz [ARGS]``（无参数 = ``lrz -y``，
否则原样把 ARGS 交给 lrz，如 ``rz -v`` 不覆盖已存在文件）/ ``sz FILE...``。
``--rz-kill-after`` 让 lrz 在启动这么多秒后被 SIGTERM（lrz 随即发 CAN 退出），模拟传输中途被取消。
被杀后 mock 丢弃随后的在途数据（读到 0.2 s 无数据为止），并在 stdout 记录
``rz killed discarded=<字节数>``，之后收到的每一行（含空行）记为 ``rz killed then line=<repr>``，
直到第一条非空命令——测试据此核对在途数据量与收尾行结束符的个数。

PKT 行格式（CRLF 结尾）::

    PKT name=<name> seq=<n> t=<epoch_ms> payload=<16 hex> crc=<4 hex>

crc 为 payload 原始 8 字节的 CRC16-CCITT（poly 0x1021，init 0xFFFF，即 CCITT-FALSE）。
"""

from __future__ import annotations

import argparse
import os
import shlex
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
                 files_dir: str, recv_dir: str, rz_kill_after: float = 0.0) -> None:
        self.ser = ser
        self.name = name
        self.interval = interval
        self.files_dir = files_dir
        self.recv_dir = recv_dir
        self.rz_kill_after = rz_kill_after
        self.lock = threading.Lock()      # 保护所有写
        self.stream = threading.Event()   # 置位 = 允许周期包
        self.stream.set()
        self.stop = threading.Event()
        self.seq = 0
        self.after_kill = False           # rz 被杀后：记录收到的行，直到第一条非空命令

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
                cmd = [exe, *args] if label == "sz" or args else [exe, "-y"]
                proc = subprocess.Popen(cmd, stdin=fd, stdout=fd,
                                        stderr=subprocess.DEVNULL, cwd=cwd)
                killer = None
                if label == "rz" and self.rz_kill_after > 0:
                    killer = threading.Timer(self.rz_kill_after, proc.terminate)
                    killer.start()
                rc = proc.wait()
                killed = bool(killer) and not killer.is_alive() and rc != 0
                if killer:
                    killer.cancel()
            finally:
                os.set_blocking(fd, was_blocking)
                if was_streaming:          # 恢复传输前的 stream 状态
                    self.stream.set()
            if killed:
                # 被杀的 rz 留下的在途数据：像真实 UART 那样照收不阻塞，但不当命令执行——
                # 读到连续 200 ms 无数据为止并丢弃（否则仿真里 mock 会逐行回显这些二进制垃圾），
                # 丢弃量记到 stdout，供测试核对它不超过发送方的窗口
                quiet = time.monotonic()
                discarded = 0
                while time.monotonic() - quiet < 0.2:
                    if data := self.ser.read(4096):
                        discarded += len(data)
                        quiet = time.monotonic()
                print(f"rz killed discarded={discarded}", flush=True)
                self.after_kill = True
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
            try:
                args = shlex.split(rest)
            except ValueError as exc:
                self.write_line(f"ERR rz: {exc}")
                return
            self._transfer(("lrz", "rz"), args, self.recv_dir, "rz")
        elif cmd == "sz":
            try:
                args = shlex.split(rest)  # 像 shell 一样处理引号（文件名可含空格）
            except ValueError as exc:
                self.write_line(f"ERR sz: {exc}")
                return
            self._transfer(("lsz", "sz"), args, self.files_dir, "sz")
        else:
            self.write_line(f"ERR unknown: {line.strip()[:60]}")  # 像 shell：只回显开头

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
                    # CRLF 算一个行结束（否则 "rz\r\n" 的 \n 会在传输结束后冒出一个空行）
                    end = idx + 2 if buf[idx:idx + 2] == b"\r\n" else idx + 1
                    raw, buf = buf[:idx], buf[end:]
                    if self.after_kill:
                        print(f"rz killed then line={raw!r}", flush=True)
                        self.after_kill = not raw.strip()
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
    ap.add_argument("--rz-kill-after", type=float, default=0.0)
    a = ap.parse_args(argv)
    ser = serial.Serial(a.device, a.baud, timeout=0.05)
    try:
        MockDevice(ser, a.name, a.interval, a.files_dir, a.recv_dir, a.rz_kill_after).run()
    except KeyboardInterrupt:
        pass
    finally:
        ser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
