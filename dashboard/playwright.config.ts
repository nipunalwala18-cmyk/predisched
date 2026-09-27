import { defineConfig } from "@playwright/test";

// Smoke test and screenshots against a running dev server (npm run dev) and dashboard API.
// Uses the installed Edge, so no browser download is needed.
export default defineConfig({
  testDir: "e2e",
  timeout: 60_000,
  use: {
    baseURL: (globalThis as any).process?.env?.DASHBOARD_URL ?? "http://localhost:5173",
    channel: "msedge",
    viewport: { width: 1440, height: 900 },
  },
});
