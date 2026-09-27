import { expect, test } from "@playwright/test";
import { PAGES } from "./pages";

// Writes docs/img/dashboard-<page>.png. With CHAOS=1 it also kills the primary from the Chaos page
// and captures the election marker on the Overview charts.
const OUT = "../docs/img";
const env = (globalThis as { process?: { env: Record<string, string | undefined> } }).process?.env ?? {};

test.describe.configure({ mode: "serial" });

for (const [path, name] of PAGES) {
  test(`screenshot ${name}`, async ({ page }) => {
    await page.goto(path);
    await page.waitForLoadState("load");
    await page.waitForTimeout(4000);
    await page.screenshot({ path: `${OUT}/dashboard-${name}.png`, fullPage: true });
  });
}

test("task drawer with an explained decision", async ({ page }) => {
  await page.goto("/tasks");
  const rows = page.getByTestId("task-table").getByRole("button");
  await rows.first().waitFor();
  // Open completed predictive tasks until one has a per-worker breakdown.
  const n = Math.min(await rows.count(), 15);
  for (let i = 0; i < n; i++) {
    const row = rows.nth(i);
    const text = await row.textContent();
    if (!text?.includes("COMPLETED") || !text.includes("predictive")) continue;
    await row.click();
    const explained = await page.getByTestId("explainer").getByText("chosen")
      .waitFor({ timeout: 4000 }).then(() => true, () => false);
    if (explained) {
      await page.waitForTimeout(800);
      await page.screenshot({ path: `${OUT}/dashboard-task-drawer.png` });
      return;
    }
    await page.keyboard.press("Escape");
  }
  throw new Error("no explained decision among the first rows");
});

test("kill the primary and watch the failover", async ({ page }) => {
  test.skip(env.CHAOS !== "1", "set CHAOS=1 to inject a fault");
  await page.goto("/chaos");
  const leader = page.getByTestId("leader");
  await expect(leader).toHaveText(/^S\d$/);
  const before = await leader.textContent();
  await page.getByRole("button", { name: "Kill primary" }).click();
  await page.getByRole("button", { name: "Confirm" }).click();
  await page.getByTestId("chaos-result").waitFor();
  // A backup wins the election and the top bar follows it.
  await expect(leader).not.toHaveText(before ?? "", { timeout: 30_000 });
  await expect(leader).toHaveText(/^S\d$/, { timeout: 30_000 });
  await page.waitForTimeout(3000);
  await page.screenshot({ path: `${OUT}/dashboard-chaos-after-kill.png`, fullPage: true });
  // Navigate in-app (key 1) so the stream buffer, and its markers, survive.
  await page.keyboard.press("1");
  await page.waitForTimeout(6000);
  await page.screenshot({ path: `${OUT}/dashboard-overview-after-kill.png`, fullPage: true });
  await page.keyboard.press("2");
  await page.waitForTimeout(4000);
  await page.screenshot({ path: `${OUT}/dashboard-cluster-after-kill.png`, fullPage: true });
});

test("demo mode runs its four steps once", async ({ page }) => {
  test.skip(env.DEMO !== "1", "set DEMO=1 to run the scripted demo (it kills the primary)");
  test.setTimeout(120_000);
  await page.goto("/");
  await page.getByRole("button", { name: "Demo" }).click();
  const caption = page.getByTestId("demo-caption");
  await expect(caption).toContainText("1/4", { timeout: 10_000 });
  await expect(caption).toContainText("2/4", { timeout: 20_000 });
  await page.waitForTimeout(6000);
  await page.screenshot({ path: `${OUT}/dashboard-demo.png` });
  await expect(caption).toContainText("3/4", { timeout: 20_000 });
  await expect(caption).toContainText("4/4", { timeout: 20_000 });
  await expect(caption).toContainText("Demo finished.", { timeout: 20_000 });
  await expect(caption).not.toContainText("step failed");
});
