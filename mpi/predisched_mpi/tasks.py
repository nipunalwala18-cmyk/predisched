"""Python versions of four PrediSched task types, with the Java executors' exact outputs.

Same ``key=value, ...`` input as the Java workers and the same result strings, so a batch run under
MPI can be cross-checked against the scheduler. ``testdata/task-outputs.json`` holds fixed cases
that both this module's tests and ``TaskOutputsFixtureTest`` (Java) assert.

    CPU_TASK          n=<2..100000000>            primes below n (sieve)
    HASH_TASK         rounds=<1..20000000>        SHA-256 chained ``rounds`` times
    MONTE_CARLO_TASK  samples=<1..>, [seed=42]    pi estimate from java.util.Random(seed)
    SLEEP_TASK        ms=<0..60000>, [failRate, seed]
    MATRIX_TASK       size=<1..1000>, [seed=42]   checksum of A @ B (exact; slow in pure Python,
                                                  so only for fixtures; matmul.py is the fast path)
"""

from __future__ import annotations

import hashlib
import time

HASH_PAYLOAD = b"predisched-hash-payload"
SUPPORTED = ("CPU_TASK", "HASH_TASK", "MONTE_CARLO_TASK", "SLEEP_TASK")


class TaskError(ValueError):
    """Bad input, reported like the Java executors' ``bad input`` failures."""


def parse_input(text: str) -> dict[str, str]:
    """``InputParser.parse``: comma-separated ``key=value`` segments, whitespace trimmed."""
    params: dict[str, str] = {}
    if not text or not text.strip():
        return params
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        eq = part.find("=")
        if eq <= 0 or eq == len(part) - 1:
            raise TaskError(f"segment must be key=value, got '{part}'")
        params[part[:eq].strip()] = part[eq + 1:].strip()
    return params


def _int(params: dict[str, str], key: str, low: int, high: int, default: int | None = None) -> int:
    if key not in params:
        if default is None:
            raise TaskError(f"missing required key '{key}'")
        return default
    try:
        value = int(params[key])
    except ValueError:
        raise TaskError(f"{key} must be an integer (got '{params[key]}')") from None
    if not low <= value <= high:
        raise TaskError(f"{key} must be between {low} and {high} (got {value})")
    return value


class JavaRandom:
    """``java.util.Random``: the 48-bit LCG, so seeded draws equal the Java worker's."""

    _MULT = 0x5DEECE66D
    _MASK = (1 << 48) - 1

    def __init__(self, seed: int):
        self._seed = (seed ^ self._MULT) & self._MASK

    def _next(self, bits: int) -> int:
        self._seed = (self._seed * self._MULT + 0xB) & self._MASK
        return self._seed >> (48 - bits)

    def next_double(self) -> float:
        return ((self._next(26) << 27) + self._next(27)) * (1.0 / (1 << 53))


def java_random_doubles(seed: int, count: int):
    """``count`` successive ``new Random(seed).nextDouble()`` values as a NumPy array, fast.

    Each double takes two LCG steps. The first block of states is stepped in Python, then whole
    blocks jump ahead at once with the closed form s[k+B] = A_B * s[k] + C_B (mod 2^48); uint64
    products wrap mod 2^64, which keeps them right mod 2^48.
    """
    import numpy as np

    mult, mask, block = JavaRandom._MULT, JavaRandom._MASK, 4096
    steps = 2 * count
    state = (seed ^ mult) & mask
    first = []
    for _ in range(min(block, steps)):
        state = (state * mult + 0xB) & mask
        first.append(state)
    a_b, c_b = 1, 0
    for _ in range(block):
        a_b, c_b = (a_b * mult) & mask, (c_b * mult + 0xB) & mask
    rows = -(-steps // block)
    states = np.empty((rows, block), dtype=np.uint64)
    states[0, :len(first)] = first
    a_u, c_u, m_u = np.uint64(a_b), np.uint64(c_b), np.uint64(mask)
    for r in range(1, rows):
        states[r] = (states[r - 1] * a_u + c_u) & m_u
    flat = states.reshape(-1)[:steps]
    hi = (flat[0::2] >> np.uint64(22)).astype(np.float64)
    lo = (flat[1::2] >> np.uint64(21)).astype(np.float64)
    return (hi * 134217728.0 + lo) * (1.0 / (1 << 53))


def matrices(size: int, seed: int = 42):
    """A and B as MatrixTaskExecutor fills them: Random(seed), alternating a[i][j], b[i][j]."""
    values = java_random_doubles(seed, 2 * size * size)
    return values[0::2].reshape(size, size), values[1::2].reshape(size, size)


def matrix_checksum_exact(size: int, seed: int = 42) -> float:
    """MatrixTaskExecutor's sequential checksum bit for bit: same draws, same summation order."""
    rnd = JavaRandom(seed)
    a = [[0.0] * size for _ in range(size)]
    b = [[0.0] * size for _ in range(size)]
    for i in range(size):
        for j in range(size):
            a[i][j] = rnd.next_double()
            b[i][j] = rnd.next_double()
    total = 0.0
    for i in range(size):
        row = a[i]
        for j in range(size):
            acc = 0.0
            for k in range(size):
                acc += row[k] * b[k][j]
            total += acc
    return total


def matrix_task(params: dict[str, str]) -> str:
    size = _int(params, "size", 1, 1_000)
    seed = _int(params, "seed", 0, 2**63 - 1, default=42)
    threads = _int(params, "threads", 1, 64, default=1)
    return f"size={size} threads={threads} checksum={matrix_checksum_exact(size, seed):.6f}"


def java_string_hash(text: str) -> int:
    """``String.hashCode`` (UTF-16 code units, 32-bit signed)."""
    h = 0
    data = text.encode("utf-16-be")
    for i in range(0, len(data), 2):
        h = (31 * h + int.from_bytes(data[i:i + 2], "big")) & 0xFFFFFFFF
    return h - (1 << 32) if h >= 1 << 31 else h


def count_primes_below(n: int) -> int:
    if n < 3:
        return 0
    composite = bytearray(n)
    count = 0
    for i in range(2, n):
        if not composite[i]:
            count += 1
            if i * i < n:
                composite[i * i::i] = b"\x01" * len(range(i * i, n, i))
    return count


def cpu_task(params: dict[str, str]) -> str:
    n = _int(params, "n", 2, 100_000_000)
    return f"primes_below_{n}={count_primes_below(n)}"


def hash_task(params: dict[str, str]) -> str:
    rounds = _int(params, "rounds", 1, 20_000_000)
    digest = HASH_PAYLOAD
    for _ in range(rounds):
        digest = hashlib.sha256(digest).digest()
    return f"rounds={rounds} digest={digest.hex()}"


def monte_carlo_task(params: dict[str, str]) -> str:
    samples = _int(params, "samples", 1, 500_000_000)
    seed = _int(params, "seed", 0, 2**63 - 1, default=42)
    rnd = JavaRandom(seed)
    inside = 0
    for _ in range(samples):
        x = rnd.next_double()
        y = rnd.next_double()
        if x * x + y * y <= 1.0:
            inside += 1
    return f"samples={samples} seed={seed} inside={inside} pi_estimate={4.0 * inside / samples:.6f}"


def sleep_task(params: dict[str, str], raw_input: str, attempt: int = 1) -> str:
    ms = _int(params, "ms", 0, 60_000)
    fail_rate = float(params.get("failRate", "0"))
    time.sleep(ms / 1000.0)
    if fail_rate > 0:
        # Same draw as SleepTaskExecutor on its first attempt: Random(seed + attempt).
        seed = int(params["seed"]) if "seed" in params else java_string_hash(raw_input)
        draw = JavaRandom(seed + attempt).next_double()
        if draw < fail_rate:
            raise TaskError(f"injected failure (failRate={fail_rate:.2f}, draw={draw:.3f})")
    return f"slept_ms={ms}"


def execute(task_type: str, text: str) -> str:
    """Runs one task; returns its result string or raises ``TaskError``."""
    params = parse_input(text)
    if task_type == "CPU_TASK":
        return cpu_task(params)
    if task_type == "HASH_TASK":
        return hash_task(params)
    if task_type == "MONTE_CARLO_TASK":
        return monte_carlo_task(params)
    if task_type == "SLEEP_TASK":
        return sleep_task(params, text)
    if task_type == "MATRIX_TASK":
        return matrix_task(params)
    raise TaskError(f"{task_type} is not supported under MPI (supported: {', '.join(SUPPORTED)})")


def input_size(task_type: str, text: str) -> int:
    """The size knob of a task (n, rounds, samples, ms), as ``TaskInputSpec.inputSize``."""
    key = {"CPU_TASK": "n", "HASH_TASK": "rounds", "MONTE_CARLO_TASK": "samples",
           "SLEEP_TASK": "ms"}.get(task_type)
    try:
        return int(parse_input(text).get(key, 0)) if key else 0
    except (TaskError, ValueError):
        return 0
