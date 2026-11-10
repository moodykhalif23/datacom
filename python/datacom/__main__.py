import os
import subprocess
import sys

from . import binary


def main():
    exe = binary()
    if os.name == "nt":
        sys.exit(subprocess.call([exe, *sys.argv[1:]]))
    os.execv(exe, [exe, *sys.argv[1:]])


if __name__ == "__main__":
    main()
