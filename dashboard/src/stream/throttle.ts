/**
 * Calls {@code fn} at most {@code perSecond} times a second (spec §15.11: ~4 updates/s).
 * Calls in between are coalesced into one trailing call, so the last update is never lost.
 */
export function throttle(
  fn: () => void,
  perSecond: number,
  now: () => number = () => Date.now(),
  schedule: (cb: () => void, ms: number) => unknown = (cb, ms) => setTimeout(cb, ms),
): () => void {
  const interval = 1000 / perSecond;
  let last = -Infinity;
  let pending = false;
  const run = () => {
    pending = false;
    last = now();
    fn();
  };
  return () => {
    if (pending) return;
    const wait = last + interval - now();
    if (wait <= 0) {
      run();
    } else {
      pending = true;
      schedule(run, wait);
    }
  };
}
