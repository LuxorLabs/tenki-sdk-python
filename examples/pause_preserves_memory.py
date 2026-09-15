"""Prove that pausing a sandbox preserves its memory, not just its disk.

Starts a process holding a multi-megabyte buffer that is never written to disk,
pauses the sandbox, resumes it, and re-reads the buffer from the same process.

    TENKI_API_KEY=... python examples/pause_preserves_memory.py
"""

import textwrap
import time

from tenki import Client

# Hashes the buffer once at startup, then again on SIGUSR1 so we can compare the
# same bytes before and after the pause. Only the hashes ever touch the disk.
GUEST = textwrap.dedent(
    """
    import hashlib, os, signal, time
    blob = os.urandom(8 * 1024 * 1024)
    digest = lambda: hashlib.sha256(blob).hexdigest()
    open("/home/tenki/hash_before", "w").write(digest())
    signal.signal(signal.SIGUSR1, lambda *_: open("/home/tenki/hash_after", "w").write(digest()))
    while True:
        time.sleep(1)
    """
)


def main() -> None:
    client = Client()
    sb = client.create(cpu_cores=2, memory_mb=2048, name="pause-preserves-memory")
    print(f"sandbox {sb.id}")

    def sh(command: str) -> str:
        return sb.exec("bash", "-lc", command).stdout.decode().strip()

    try:
        sb.fs.write_text("/home/tenki/mem.py", GUEST)
        sh("setsid nohup python3 /home/tenki/mem.py >/dev/null 2>&1 </dev/null &")
        time.sleep(2)

        pid = sh("pgrep -f mem.py")
        before = sh("cat /home/tenki/hash_before")
        uptime_before = sh("cut -d. -f1 /proc/uptime")
        print(f"pid {pid} holding 8 MiB at sha256 {before[:16]}...")

        sb.pause()
        print("paused; waiting 30s to make the gap obvious")
        time.sleep(30)
        sb.resume()

        sh(f"kill -USR1 {pid}")
        time.sleep(1)
        after = sh("cat /home/tenki/hash_after")
        uptime_after = sh("cut -d. -f1 /proc/uptime")

        print(f"same process alive : {sh(f'kill -0 {pid} && echo yes') == 'yes'}")
        print(f"buffer unchanged   : {after == before}")
        print(f"guest clock froze  : {uptime_before}s -> {uptime_after}s across a ~30s pause")
    finally:
        sb.close()


if __name__ == "__main__":
    main()
