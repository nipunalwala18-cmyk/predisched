import "@testing-library/jest-dom/vitest";

// Recharts and React Flow measure the DOM; jsdom has no ResizeObserver.
class ResizeObserverStub {
  observe() {}
  unobserve() {}
  disconnect() {}
}
(globalThis as any).ResizeObserver = ResizeObserverStub;
