import { mkdir, copyFile, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import * as esbuild from "esbuild";

const rootDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const outFile = resolve(rootDir, "domain_interface_explorer/static/vendor/molstar-bridge.js");
const cssOutFile = resolve(rootDir, "domain_interface_explorer/static/vendor/molstar.css");
const h264Stub = resolve(rootDir, "frontend/molstar-h264-stub.ts");

const optionalBrowserStubs = {
  name: "molstar-optional-browser-stubs",
  setup(build) {
    build.onResolve({ filter: /^h264-mp4-encoder$/ }, () => ({ path: h264Stub }));
  },
};

await mkdir(dirname(outFile), { recursive: true });
await esbuild.build({
  entryPoints: [resolve(rootDir, "frontend/molstar-bridge.ts")],
  bundle: true,
  format: "esm",
  platform: "browser",
  target: "es2020",
  outfile: outFile,
  loader: {
    ".jpg": "dataurl",
    ".png": "dataurl",
    ".svg": "dataurl",
    ".woff": "dataurl",
    ".woff2": "dataurl",
    ".ttf": "dataurl",
  },
  plugins: [optionalBrowserStubs],
});

await preserveExistingVendorBehavior(outFile);
await copyFile(
  resolve(rootDir, "node_modules/molstar/build/viewer/molstar.css"),
  cssOutFile
);

function replaceRequired(source, search, replacement, label) {
  if (!source.includes(search)) {
    throw new Error(`Could not find ${label} in generated Mol* bundle.`);
  }
  return source.replace(search, replacement);
}

async function preserveExistingVendorBehavior(bundlePath) {
  let bundle = await readFile(bundlePath, "utf8");
  bundle = replaceRequired(
    bundle,
    `  dragPan: Binding([
    Trigger(B16.Flag.Secondary, M.create()),
    Trigger(B16.Flag.Primary, M.create({ control: true }))
  ], "Pan", "Drag using \${triggers}"),
  dragZoom: Binding.Empty,
  dragFocus: Binding([Trigger(B16.Flag.Forth, M.create())], "Focus", "Drag using \${triggers}"),
  dragFocusZoom: Binding([Trigger(B16.Flag.Auxilary, M.create())], "Focus and zoom", "Drag using \${triggers}"),
  scrollZoom: Binding([Trigger(B16.Flag.Auxilary, M.create())], "Zoom", "Scroll using \${triggers}"),
  scrollFocus: Binding([Trigger(B16.Flag.Auxilary, M.create({ shift: true }))], "Clip", "Scroll using \${triggers}"),
  scrollFocusZoom: Binding.Empty,
  keyMoveForward: Binding([
    Key("KeyW"),
    Key("GamepadUp")
  ], "Move forward", "Press \${triggers}"),
  keyMoveBack: Binding([
    Key("KeyS"),
    Key("GamepadDown")
  ], "Move back", "Press \${triggers}"),
  keyMoveLeft: Binding([Key("KeyA")], "Move left", "Press \${triggers}"),
  keyMoveRight: Binding([Key("KeyD")], "Move right", "Press \${triggers}"),
  keyMoveUp: Binding([Key("KeyR")], "Move up", "Press \${triggers}"),
  keyMoveDown: Binding([Key("KeyF")], "Move down", "Press \${triggers}"),
  keyRollLeft: Binding([Key("KeyQ")], "Roll left", "Press \${triggers}"),
  keyRollRight: Binding([Key("KeyE")], "Roll right", "Press \${triggers}"),
  keyPitchUp: Binding([Key("ArrowUp", M.create({ shift: true }))], "Pitch up", "Press \${triggers}"),
  keyPitchDown: Binding([Key("ArrowDown", M.create({ shift: true }))], "Pitch down", "Press \${triggers}"),
  keyYawLeft: Binding([Key("ArrowLeft", M.create({ shift: true }))], "Yaw left", "Press \${triggers}"),
  keyYawRight: Binding([Key("ArrowRight", M.create({ shift: true }))], "Yaw right", "Press \${triggers}"),
  boostMove: Binding([Key("ShiftLeft")], "Boost move", "Press \${triggers}"),
  enablePointerLock: Binding([Key("Space", M.create({ control: true }))], "Enable pointer lock", "Press \${triggers}")`,
    `  dragPan: Binding([
    Trigger(B16.Flag.Auxilary, M.create())
  ], "Pan", "Drag using \${triggers}"),
  dragZoom: Binding.Empty,
  dragFocus: Binding([Trigger(B16.Flag.Forth, M.create())], "Focus", "Drag using \${triggers}"),
  dragFocusZoom: Binding.Empty,
  scrollZoom: Binding([Trigger(B16.Flag.Auxilary, M.create())], "Zoom", "Scroll using \${triggers}"),
  scrollFocus: Binding.Empty,
  scrollFocusZoom: Binding.Empty,
  keyMoveForward: Binding.Empty,
  keyMoveBack: Binding.Empty,
  keyMoveLeft: Binding.Empty,
  keyMoveRight: Binding.Empty,
  keyMoveUp: Binding.Empty,
  keyMoveDown: Binding.Empty,
  keyRollLeft: Binding.Empty,
  keyRollRight: Binding.Empty,
  keyPitchUp: Binding.Empty,
  keyPitchDown: Binding.Empty,
  keyYawLeft: Binding.Empty,
  keyYawRight: Binding.Empty,
  boostMove: Binding.Empty,
  enablePointerLock: Binding.Empty`,
    "existing trackball bindings",
  );
  bundle = bundle.replace("// frontend/molstar-h264-stub.ts", "// frontend/molstar-h264-stub.js");
  bundle = replaceRequired(
    bundle,
    `export {
  Color,
  MolScriptBuilder as MS,
  PluginCommands,
  PluginConfig,
  element_exports as StructureElement,
  StructureProperties,
  Viewer
};`,
    `export {
  Color,
  MolScriptBuilder as MS,
  PluginConfig,
  PluginCommands,
  element_exports as StructureElement,
  StructureProperties,
  Viewer
};`,
    "existing Mol* bridge export order",
  );
  await writeFile(bundlePath, bundle);
}
