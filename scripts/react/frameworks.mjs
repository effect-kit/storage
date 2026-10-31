import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";

import { chromium } from "playwright";
import { build } from "vite";

import { protocolServer } from "./server.mjs";

export async function checkFrameworks(directory) {
  await rm(".artifacts/react/router-error.json", { force: true });
  const manifest = JSON.parse(await readFile(join(directory, "package.json"), "utf8"));
  await Promise.all(
    ["next", "@tanstack/react-router"].map(async (name) => {
      const installed = JSON.parse(
        await readFile(resolve("packages/storage/node_modules", name, "package.json"), "utf8"),
      );
      manifest.dependencies[name] = installed.version;
    }),
  );
  await writeFile(join(directory, "package.json"), JSON.stringify(manifest));
  execFileSync(
    "pnpm",
    [
      "--ignore-workspace",
      "install",
      "--offline",
      "--ignore-scripts",
      "--config.auto-install-peers=false",
    ],
    { cwd: directory, stdio: "pipe" },
  );
  const app = join(directory, "next-app");
  await mkdir(join(app, "app"), { recursive: true });
  await writeFile(join(app, "package.json"), JSON.stringify({ private: true, type: "module" }));
  await writeFile(join(app, "next.config.mjs"), "export default { experimental: { cpus: 1 } };");
  await writeFile(
    join(app, "app/layout.js"),
    'import { createElement as h } from "react"; export default function Layout({children}) { return h("html", {lang:"en"}, h("body",{}, children)); }',
  );
  await writeFile(
    join(app, "app/page.js"),
    'import { createElement as h } from "react"; import Probe from "./probe.js"; export default function Page() { return h(Probe); }',
  );
  await writeFile(
    join(app, "app/probe.js"),
    `"use client";
import { createElement as h } from "react";
import { RegistryProvider } from "@effect/atom-react";
import { UploadClient } from "@effect-kit/storage/client";
import { UploadAtoms } from "@effect-kit/storage/client/atoms";
import { useUpload } from "@effect-kit/storage/react";
const slot = UploadAtoms.make({ layer: UploadClient.layer({baseUrl:"/storage"}), purpose:"test", context:{} });
function View() { return h("output",{},useUpload(slot).state.phase); }
export default function Probe() { return h(RegistryProvider,{},h(View)); }
`,
  );
  const output = execFileSync(
    process.execPath,
    [join(directory, "node_modules/next/dist/bin/next"), "build", app, "--webpack"],
    {
      cwd: app,
      encoding: "utf8",
      stdio: "pipe",
      timeout: 180000,
      env: { ...process.env, NEXT_TELEMETRY_DISABLED: "1" },
    },
  );
  await writeFile(".artifacts/react/next-build.log", output);
  const html = await readFile(join(app, ".next/server/app/index.html"), "utf8");
  assert(html.includes("idle"));

  await writeFile(
    join(directory, "router.mjs"),
    `
import { Effect } from "effect";
import { AtomRegistry } from "effect/reactivity";
import { createElement as h, useEffect } from "react";
import { createRoot } from "react-dom/client";
import { RegistryContext } from "@effect/atom-react";
import { createRootRoute, createRoute, createRouter, createMemoryHistory, RouterProvider, Outlet } from "@tanstack/react-router";
import { UploadClient } from "@effect-kit/storage/client";
import { UploadAtoms } from "@effect-kit/storage/client/atoms";
import { useUpload } from "@effect-kit/storage/react";
const registry = AtomRegistry.make();
const slot = UploadAtoms.make({layer:UploadClient.layer({baseUrl:"/storage",headers:()=>Effect.succeed({authorization:"Bearer fresh"})}),purpose:"test",context:{},strategy:"resumable"});
function View(){const upload=useUpload(slot);useEffect(()=>{globalThis.routerProbe.upload=upload;});return h("output",{},upload.state.phase);}
const rootRoute=createRootRoute({component:()=>h(Outlet)});
const first=createRoute({getParentRoute:()=>rootRoute,path:"/",component:View});
const second=createRoute({getParentRoute:()=>rootRoute,path:"/other",component:View});
const router=createRouter({routeTree:rootRoute.addChildren([first,second]),history:createMemoryHistory({initialEntries:["/"]})});
globalThis.routerProbe={router,registry,snapshot:()=>registry.get(slot.state)};
createRoot(document.getElementById("root")).render(h(RegistryContext.Provider,{value:registry},h(RouterProvider,{router})));
`,
  );
  await build({
    root: directory,
    configFile: false,
    logLevel: "silent",
    define: { "process.env.NODE_ENV": JSON.stringify("production") },
    build: {
      outDir: join(directory, "router"),
      minify: false,
      lib: {
        entry: join(directory, "router.mjs"),
        formats: ["iife"],
        name: "RouterProbe",
        fileName: () => "probe.js",
      },
    },
  });
  const routerBundle = await readFile(join(directory, "router/probe.js"), "utf8");
  await writeFile(".artifacts/react/router.js", routerBundle);
  const server = await protocolServer(routerBundle);
  server.control.holdData = true;
  const browser = await chromium.launch({ headless: true });
  let page;
  const errors = [];
  try {
    page = await browser.newPage();
    page.on("pageerror", (error) => errors.push(error.stack ?? String(error)));
    page.on("console", (message) => {
      if (message.type() === "error") errors.push(message.text());
    });
    await page.goto(server.url);
    await page.waitForFunction(() => globalThis.routerProbe?.upload);
    await page.evaluate(() =>
      routerProbe.upload.start(new File(["hello"], "route.txt", { type: "text/plain" })),
    );
    await page.waitForFunction(() => routerProbe.snapshot().phase === "uploading");
    await page.evaluate(() => routerProbe.router.navigate({ to: "/other" }));
    server.release();
    await page.waitForFunction(() => routerProbe.snapshot().phase === "completed");
    assert.equal(server.created(), 1);
    await page.evaluate(() => routerProbe.registry.dispose());
  } catch (error) {
    await writeFile(
      ".artifacts/react/router-error.json",
      JSON.stringify({ errors, body: await page?.locator("body").textContent() }, null, 2),
    );
    throw error;
  } finally {
    await browser.close();
    await server.close();
  }
}
