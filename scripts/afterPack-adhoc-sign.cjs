const { execFileSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

function signApp(appPath) {
  execFileSync("codesign", ["--force", "--deep", "--sign", "-", appPath], {
    stdio: "inherit",
  });
}

exports.default = async function afterPack(context) {
  if (process.platform !== "darwin") return;
  if (!context || !context.appOutDir) return;

  const entries = fs.readdirSync(context.appOutDir, { withFileTypes: true });
  const appBundles = entries
    .filter((entry) => entry.isDirectory() && entry.name.endsWith(".app"))
    .map((entry) => path.join(context.appOutDir, entry.name));

  if (appBundles.length === 0) {
    console.warn("[adhoc-sign] No .app bundle found in", context.appOutDir);
    return;
  }

  for (const appPath of appBundles) {
    const cliLauncherPath = path.join(
      appPath,
      "Contents",
      "Resources",
      "bin",
      "no-bullshit-rss",
    );
    if (!fs.existsSync(cliLauncherPath)) {
      throw new Error(`[adhoc-sign] CLI launcher missing at ${cliLauncherPath}`);
    }
    fs.chmodSync(cliLauncherPath, 0o755);
    console.log(`[adhoc-sign] bundled CLI launcher ${cliLauncherPath}`);

    console.log(`[adhoc-sign] codesign -s - ${appPath}`);
    signApp(appPath);
  }
};
