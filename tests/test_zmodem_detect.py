"""ZMODEM 起始头检测器测试（纯字节逻辑，无串口）。"""

from __future__ import annotations

import pytest

from serialhub.zmodem import ABORT_SEQ, Detection, ZmodemDetector

ZRQINIT = b"**\x18B00000000000000\r\x8a\x11"  # 设备跑 sz（下载）
ZRINIT = b"**\x18B0100000023be50\r\x8a\x11"   # 设备跑 rz（上传）


def test_abort_seq_is_8_can_8_bs() -> None:
    assert ABORT_SEQ == b"\x18" * 8 + b"\x08" * 8


def test_header_in_one_chunk() -> None:
    det = ZmodemDetector()
    text, hit = det.feed(ZRQINIT)
    assert text == b""
    assert hit == Detection(direction="download", initial=ZRQINIT)


@pytest.mark.parametrize("cut", range(1, len(ZRQINIT)))
def test_header_split_at_every_offset(cut: int) -> None:
    det = ZmodemDetector()
    text1, hit1 = det.feed(b"abc" + ZRQINIT[:cut])
    assert hit1 is None or cut >= 6  # 头 6 字节齐了才可能命中
    if hit1 is None:
        text2, hit2 = det.feed(ZRQINIT[cut:])
        assert text1 + text2 == b"abc"
        assert hit2 is not None
        assert hit2.direction == "download"
        assert hit2.initial == ZRQINIT
    else:
        assert text1 == b"abc"
        assert hit1.initial == ZRQINIT[:cut]


def test_text_before_header_returned() -> None:
    det = ZmodemDetector()
    text, hit = det.feed(b"rz\r" + ZRQINIT)
    assert text == b"rz\r"
    assert hit is not None and hit.initial == ZRQINIT


def test_trailing_star_released_by_flush() -> None:
    det = ZmodemDetector()
    text, hit = det.feed(b"price **")
    assert hit is None
    assert text == b"price "
    assert det.flush() == b"**"
    assert det.flush() == b""


def test_partial_prefix_not_followed_by_header_is_released_on_next_feed() -> None:
    det = ZmodemDetector()
    text1, _ = det.feed(b"a**\x18")
    text2, hit = det.feed(b"zz\n")
    assert hit is None
    assert text1 + text2 == b"a**\x18zz\n"


def test_other_frame_type_not_detected() -> None:
    det = ZmodemDetector()
    text, hit = det.feed(b"**\x18B02xyz\n")
    assert hit is None
    assert text + det.flush() == b"**\x18B02xyz\n"


@pytest.mark.parametrize(
    "frame,direction", [(ZRQINIT, "download"), (ZRINIT, "upload")]
)
def test_direction_mapping(frame: bytes, direction: str) -> None:
    _, hit = ZmodemDetector().feed(frame)
    assert hit is not None and hit.direction == direction
