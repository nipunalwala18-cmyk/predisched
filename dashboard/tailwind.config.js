/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  darkMode: ["class"],
  theme: {
    extend: {
      colors: {
        surface: "var(--surface)",
        panel: "var(--panel)",
        ink: "var(--ink)",
        muted: "var(--muted)",
        rule: "var(--rule)",
        accent: "var(--accent)",
        good: "var(--good)",
        warn: "var(--warn)",
        bad: "var(--bad)",
      },
    },
  },
  plugins: [],
};
