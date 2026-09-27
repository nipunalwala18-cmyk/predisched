import { expect, test } from "@playwright/test";
import { PAGES } from "./pages";

// Every page loads against the live API without a console error or an error panel.
// Recharts 2.12 logs a React dev-mode deprecation warning (defaultProps on XAxis/YAxis) through
// console.error; it is the library's, absent from production builds, so it is ignored here.
const IGNORED = [/Support for defaultProps will be removed/];
for (const [path, name] of PAGES) {
  test(`${name} page loads cleanly`, async ({ page }) => {
    const errors: string[] = [];
    page.on("console", (m) => { if (m.type() === "error" && !IGNORED.some((r) => r.test(m.text()))) errors.push(m.text()); });
    page.on("pageerror", (e) => errors.push(e.message));
    await page.goto(path);
    await expect(page.locator("main")).toBeVisible();
    await page.waitForLoadState("load");
    await page.waitForTimeout(1500);
    await expect(page.getByText("Could not load")).toHaveCount(0);
    expect(errors).toEqual([]);
  });
}
