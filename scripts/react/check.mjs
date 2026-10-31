import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import { chromium } from "playwright";

import { checkReactConsumer } from "./consumer.ts";
import { checkFrameworks } from "./frameworks.mjs";
import { protocolServer } from "./server.mjs";

const artifact = JSON.parse(await readFile(".artifacts/artifact-check.json", "utf8"));
const results = [];
const pass = (name) => {
  results.push(name);
  process.stdout.write(`PASS ${name}\n`);
};
await writeFile(".artifacts/react-check.json", JSON.stringify({ status: "running" }));
try {
  await checkReactConsumer(resolve(artifact.archive), async (directory, bundle) => {
    const server = await protocolServer(bundle);
    const errors = [];
    server.onError((error) => errors.push(String(error)));
    const browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    page.on("pageerror", (error) => errors.push(String(error)));
    const phase = async (expected, index = 0) =>
      page.waitForFunction(
        ({ expected: target, index: slot }) => globalThis.probe?.snapshot(slot).phase === target,
        { expected, index },
        { timeout: 15000 },
      );
    const start = async (index = 0) =>
      page.evaluate((slot) => probe.views[slot].start(probe.file()), index);
    try {
      await page.goto(server.url);
      await page.waitForFunction(() => probe.views.length === 2 && probe.views[0]);
      assert.equal(server.created(), 0);
      assert(
        (await page.evaluate(() => probe.renders())) >= 4,
        "React development StrictMode did not double render the two views",
      );
      await start();
      await phase("completed");
      assert.equal(server.created(), 1);
      assert.equal(await page.locator('[data-slot="0"]').textContent(), "completed");
      pass("StrictMode idle construction and authoritative publication");

      server.control.holdData = true;
      await start();
      await page.waitForFunction(() => probe.snapshot().canPause);
      await page.evaluate(() => probe.views[0].pause());
      await phase("paused");
      assert([...server.records.values()].some((record) => record.state === "uploading"));
      server.release();
      await page.evaluate(() => probe.views[0].resume({ file: probe.file("wrong") }));
      await phase("uncertain");
      await page.evaluate(() => probe.views[0].resume({ file: probe.file() }));
      await phase("completed");
      pass("pause joins transfer; wrong-source resume rejects; offset recovery completes");

      server.control.holdData = true;
      await start();
      await page.waitForFunction(() => probe.snapshot().canPause);
      await page.evaluate(() => probe.hide());
      await page.waitForFunction(() => !probe.views[0]);
      server.release();
      await phase("completed");
      await page.evaluate(() => probe.show());
      await page.waitForFunction(() => probe.views[0]?.state.phase === "completed");
      pass("component replacement does not abandon application-owned work");

      server.control.holdData = true;
      await start();
      await start(1);
      await page.waitForFunction(() => probe.snapshot().canPause && probe.snapshot(1).canPause);
      await page.evaluate(() => probe.views[0].cancel());
      await phase("cancelled");
      assert.equal(await page.evaluate(() => probe.snapshot().status.cleanupPending), true);
      assert.equal(await page.evaluate(() => probe.snapshot(1).isRunning), true);
      server.release();
      await phase("completed", 1);
      pass("concurrent slots and cancellation with explicit cleanup debt");

      server.control.holdData = true;
      server.control.single = true;
      await start();
      await page.waitForFunction(() => probe.snapshot().phase === "uploading");
      assert.equal(await page.evaluate(() => probe.snapshot().canPause), false);
      await page.evaluate(() => probe.views[0].cancel());
      await phase("cancelled");
      server.release();
      server.control.single = false;
      pass("pause availability follows actual issued protocol");

      await page.evaluate(() => probe.token("expired"));
      await start();
      await phase("uncertain");
      await page.evaluate(() => {
        probe.token("fresh");
        probe.views[0].retry();
      });
      await phase("completed");
      pass("expired authorization uses refreshed headers on explicit recovery");

      server.control.failCreate = true;
      const before = server.created();
      await start();
      await phase("uncertain");
      assert.equal(server.created(), before + 1);
      server.control.failCreate = false;
      await page.evaluate(() => probe.views[0].retry());
      await phase("completed");
      assert.equal(server.created(), before + 1);
      pass("lost create response recovers one intent using persisted idempotency identity");

      server.control.holdData = true;
      await start();
      await page.waitForFunction(() => probe.snapshot().canPause);
      const resumeId = await page.evaluate(() => probe.snapshot().resumeId);
      const persisted = await page.evaluate(() => JSON.stringify(localStorage));
      assert(!persisted.includes("Bearer") && !persisted.includes("http://"));
      await page.evaluate(() => probe.dispose());
      server.release();
      await page.reload();
      await page.waitForFunction(() => probe.views[0]);
      await page.evaluate(
        (uploadId) => probe.views[0].resume({ uploadId, file: probe.file() }),
        resumeId,
      );
      await phase("completed");
      pass("reload reselects source from grant-free tickets");

      server.control.holdComplete = true;
      await start();
      await phase("verifying");
      assert.equal(await page.evaluate(() => probe.snapshot().isRunning), true);
      server.release();
      await phase("completed");
      pass("full transferred bytes remain verifying until publication response");

      await page.evaluate(() => probe.replaceAccount());
      await phase("idle");
      assert.equal(await page.evaluate(() => probe.snapshot().status), undefined);
      pass("replacement application registry isolates account state");
      assert.deepEqual(errors, []);
    } finally {
      await browser.close();
      await server.close();
    }
    await checkFrameworks(directory);
    pass("Next.js production server/client build and SSR idle snapshot");
    pass("TanStack Router navigation preserves one application-owned upload");
  });
  await writeFile(
    ".artifacts/react-check.json",
    JSON.stringify({ status: "passed", hostedQualified: false, results }, null, 2),
  );
  process.stdout.write(
    `React acceptance passed: ${results.length} browser/framework checks over exact packed consumers.\n`,
  );
} catch (error) {
  await writeFile(
    ".artifacts/react-check.json",
    JSON.stringify({ status: "failed", results }, null, 2),
  );
  throw error;
}
