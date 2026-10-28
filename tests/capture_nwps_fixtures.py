"""Capture the NWPS job's test fixtures (plan section 59) from real NOMADS files (network, paced):

    python tests/capture_nwps_fixtures.py <region> <YYYYMMDD> <office> <HH> <station> [<region> <day> <office> <HH> <nodata station>]

tests/fixtures/nwps/<station>.spc2d.txt      a real point's spectra file cut to its first HOURS times (the header whole)
tests/fixtures/nwps/<station>_nodata.txt     the head of a file whose point holds NODATA (the first time only)
tests/fixtures/nwps/listing_CG1.html         the cycle's CG1 directory page (the discovery parser's input)
"""
import os
import re
import sys
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "tools", "model_frames"))
import nwps_fetch as N  # noqa: E402

HOURS = 6


def cut(text, hours):
    """The file up to and including its first `hours` time blocks."""
    out, n = [], 0
    for line in text.splitlines():
        if re.match(r"^\d{8}\.\d{6}\s", line):
            n += 1
            if n > hours:
                break
        out.append(line)
    return "\n".join(out) + "\n"


def main(argv):
    region, day, office, hh, sid = argv[:5]
    day = date(int(day[:4]), int(day[4:6]), int(day[6:]))
    hh = int(hh)
    out = os.path.join(HERE, "fixtures", "nwps")
    os.makedirs(out, exist_ok=True)
    _s, body = N.request(N.spc_url(region, day, office, hh, sid))
    text = body.decode("ascii")
    open(os.path.join(out, f"{sid}.spc2d.txt"), "w", encoding="ascii", newline="\n").write(cut(text, HOURS))
    _s, page = N.request(f"{N.cycle_dir(region, day, office, hh)}/CG1/")
    open(os.path.join(out, "listing_CG1.html"), "w", encoding="utf-8", newline="\n").write(page.decode("utf-8", "replace"))
    if len(argv) >= 10:
        region2, day2, office2, hh2, sid2 = argv[5:10]
        day2 = date(int(day2[:4]), int(day2[4:6]), int(day2[6:]))
        head = N.spectra_head(N.spc_url(region2, day2, office2, int(hh2), sid2), 7000)
        open(os.path.join(out, f"{sid2}_nodata.txt"), "w", encoding="ascii", newline="\n").write(cut(head, 1))
    print("wrote", out)


if __name__ == "__main__":
    main(sys.argv[1:])
