"""Cut two real NWPS spectra (aer's 2026102806 run, captured by the G41a review) to the one hour where equal values meet:
46001 at 2026102809 (a smoothed bin with two neighbours of the same integer sum) and 46060 at 2026102920 (a partition
with two equal frequency peaks). Writes tests/fixtures/nwps/aer_46001_tie.txt and aer_46060_tptie.txt.
python -I tests/capture_nwps_ties.py <dir holding aer_2026102806_46001.txt and aer_2026102806_46060.txt>"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "fixtures", "nwps")


def cut(path, stamp):
    text = open(path, encoding="ascii").read()
    first = re.search(r"(?m)^\d{8}\.\d{6}", text).start()
    blocks = re.split(r"(?m)^(?=\d{8}\.\d{6})", text[first:])
    keep = [b for b in blocks if b.startswith(stamp)]
    assert len(keep) == 1, stamp
    return text[:first] + keep[0]


def main(src):
    for name, stamp, out in (("aer_2026102806_46001.txt", "20261028.090000", "aer_46001_tie.txt"),
                             ("aer_2026102806_46060.txt", "20261029.200000", "aer_46060_tptie.txt")):
        body = cut(os.path.join(src, name), stamp)
        with open(os.path.join(OUT, out), "w", encoding="ascii", newline="\n") as f:
            f.write(body)
        print(out, len(body))


if __name__ == "__main__":
    main(sys.argv[1])
