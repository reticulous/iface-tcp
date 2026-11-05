"""Reference vectors for test_hdlc.cpp, from Python RNS's TCPInterface.

Writes into OUTDIR:
  packets.bin   the packets, each as <u16 little-endian length><bytes>
  frames.bin    the packets framed by RNS: FLAG + HDLC.escape(pkt) + FLAG each
  stream.bin    frames.bin with junk before the first FLAG and idle FLAGs
                (empty frames) between frames, as a receiver may see
Every packet is checked to come back out of stream.bin, fed in random chunks,
through TCPInterface's HDLC read loop (reference_read below is that loop with
the socket and process_incoming taken out).

  python -I hdlc_vectors.py OUTDIR [SEED]
"""
import os
import random
import struct
import sys

from RNS.Interfaces.TCPInterface import HDLC

HW_MTU = 262144   # TCPClientInterface.HW_MTU


def reference_read(chunks):
    """TCPInterface read_loop, HDLC branch."""
    out = []
    frame_buffer = b""
    for data_in in chunks:
        frame_buffer += data_in
        flags_remaining = True
        while flags_remaining:
            frame_start = frame_buffer.find(HDLC.FLAG)
            if frame_start != -1:
                frame_end = frame_buffer.find(HDLC.FLAG, frame_start + 1)
                if frame_end != -1:
                    frame = frame_buffer[frame_start + 1:frame_end]
                    frame = frame.replace(bytes([HDLC.ESC, HDLC.FLAG ^ HDLC.ESC_MASK]), bytes([HDLC.FLAG]))
                    frame = frame.replace(bytes([HDLC.ESC, HDLC.ESC ^ HDLC.ESC_MASK]), bytes([HDLC.ESC]))
                    if len(frame) != 0:
                        out.append(frame)
                    frame_buffer = frame_buffer[frame_end:]
                else:
                    if len(frame_buffer) > HW_MTU * 2:
                        frame_buffer = b""
                    flags_remaining = False
            else:
                frame_buffer = b""
    return out


def rand_packet(rng):
    n = rng.choice([1, 2, 19, 20, 251, 253, 254, 256, 271, 275, 412, 414, 416, 435, 499, 500,
                    rng.randint(1, 500), rng.randint(1, 500), rng.randint(1, 500)])
    mode = rng.random()
    if mode < 0.1:
        return bytes([HDLC.ESC]) * n
    if mode < 0.2:
        return bytes([HDLC.FLAG]) * n
    if mode < 0.3:
        return bytes(rng.choice([HDLC.ESC, HDLC.FLAG]) for _ in range(n))
    # Mostly random, rich in the special bytes and in values that equal an
    # escaped byte (0x5D, 0x5E), in runs.
    out = bytearray()
    while len(out) < n:
        r = rng.random()
        if r < 0.25:
            out += bytes([HDLC.ESC]) * rng.randint(1, 4)
        elif r < 0.45:
            out += bytes([HDLC.FLAG]) * rng.randint(1, 4)
        elif r < 0.55:
            out += bytes([rng.choice([0x5D, 0x5E, 0x20])])
        else:
            out.append(rng.randrange(256))
    return bytes(out[:n])


def main():
    outdir = sys.argv[1]
    rng = random.Random(int(sys.argv[2]) if len(sys.argv) > 2 else 1)
    packets = [bytes([HDLC.ESC]) * 500, bytes([HDLC.FLAG]) * 500,
               bytes([HDLC.ESC, HDLC.FLAG]) * 250, bytes([HDLC.FLAG ^ HDLC.ESC_MASK, HDLC.ESC ^ HDLC.ESC_MASK]) * 250]
    packets += [rand_packet(rng) for _ in range(3000)]

    frames = b"".join(bytes([HDLC.FLAG]) + HDLC.escape(p) + bytes([HDLC.FLAG]) for p in packets)
    stream = bytearray(b"\x00\x7d\x5e junk \x7d")
    for p in packets:
        stream += bytes([HDLC.FLAG]) * rng.choice([1, 1, 1, 2, 3])
        stream += HDLC.escape(p)
        stream += bytes([HDLC.FLAG])
    stream = bytes(stream)

    for data in (frames, stream):
        chunks, i = [], 0
        while i < len(data):
            k = rng.randint(1, 700)
            chunks.append(data[i:i + k])
            i += k
        got = reference_read(chunks)
        assert got == packets, "reference decode mismatch"

    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "packets.bin"), "wb") as f:
        for p in packets:
            f.write(struct.pack("<H", len(p)) + p)
    with open(os.path.join(outdir, "frames.bin"), "wb") as f:
        f.write(frames)
    with open(os.path.join(outdir, "stream.bin"), "wb") as f:
        f.write(stream)
    print("%d packets, %d frame bytes, %d stream bytes" % (len(packets), len(frames), len(stream)))


main()
