import re
from itertools import zip_longest

import colorama as c

from cma_atlas import __version__

# 7-bit C1 ANSI sequences
ansi_escape = re.compile(
    r"""
    \x1B  # ESC
    (?:   # 7-bit C1 Fe (except CSI)
        [@-Z\\-_]
    |     # or [ for CSI, followed by a control sequence
        \[
        [0-?]*  # Parameter bytes
        [ -/]*  # Intermediate bytes
        [@-~]   # Final byte
    )
""",
    re.VERBOSE,
)


def strip_colors(string):
    return ansi_escape.sub("", string)


def combine_linewise(a, b, padding="", strip=False, align_bottom=False, fix_len=True):
    lines_a = a.split("\n")
    lines_b = b.split("\n")

    if align_bottom:
        lines_a = list(reversed(lines_a))
        lines_b = list(reversed(lines_b))

    def max_len(lines):
        return max([len(strip_colors(x)) for x in lines])

    if max_len(lines_a) > max_len(lines_b) and fix_len:
        fill = " " * max_len(lines_a)
    elif max_len(lines_b) > max_len(lines_a) and fix_len:
        fill = " " * max_len(lines_b)
    else:
        fill = ""

    result = []
    for line_a, line_b in zip_longest(lines_a, lines_b, fillvalue=fill):
        if strip:
            line_a, line_b = line_a.strip(), line_b.strip()
        line_a = line_a + padding
        result.append(line_a + line_b)

    if align_bottom:
        result = reversed(result)

    return "\n".join(result)


def make_square(logo, side="left"):
    assert side in ["left", "right"]
    lines = logo.split("\n")
    longest = max([len(strip_colors(x)) for x in lines])

    res = []
    for line in lines:
        padding = " " * (longest - len(strip_colors(line)))

        if side == "left":
            res.append(line + padding)
        elif side == "right":
            res.append(padding + line)

    return "\n".join(res)


def break_long_lines(text: str, max_len: int = 80) -> str:
    """This breaks very long lines in `text` to `max_len`.
    Only breaks at spaces.
    """
    lines = text.split("\n")
    new_lines = []

    for line in lines:
        # If the line is already ok, leave it as-is
        if len(line) <= max_len:
            new_lines.append(line)
            continue

        # If not, we have work to do
        words = line.split(" ")
        new_line = ""
        for word in words:
            if len(f"{new_line} {word}") > (max_len + 1):
                # The lstrip is there to remove the extra space at the start
                # the same goes for the +1 above
                new_lines.append(new_line.lstrip())
                new_line = ""
            new_line += f" {word}"
        new_lines.append(new_line.lstrip())

    new_text = "\n".join(new_lines)

    return new_text


INFO = break_long_lines(
    f"""
                {c.Fore.LIGHTBLUE_EX}>>--  ATLAS  --<<{c.Fore.RESET} v.{__version__}

Atlas is an adapter for many remote databases holding biological data.

Use {c.Fore.GREEN}atlas tables{c.Fore.RESET} to get a list of what data can be retrieved by atlas. Use {c.Fore.GREEN}atlas genquery{c.Fore.RESET} to select what data you want atlas to retrieve for you, and in what format. The command generates a {c.Fore.LIGHTBLACK_EX}query file{c.Fore.RESET} that is taken as input by {c.Fore.GREEN}atlas retrieve{c.Fore.RESET}; the command then retrieves the data, and adapts it to whatever output was specified.

Atlas stores a copy of the data locally (specified in the options file) as a SQLite database. This allows for re-usage of the local data without having to download sources again at a future time. Specify the {c.Fore.GREEN}--no-save{c.Fore.RESET} flag to prevent any data saved locally.
"""
)
