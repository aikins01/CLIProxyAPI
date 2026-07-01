#!/usr/bin/env node

// Exact, byte-for-byte audit that every per-model/mode system prompt family our
// Neo runtime ships (the neoPromptFamily*Gzip blobs in neo_runtime.go) still
// matches the prompt the installed Amp binary would build for that family.
//
// Everything is derived from the binary's own stable anchors rather than
// hard-coded minified identifiers, so the audit survives Amp's re-minification:
//   - family -> builder function: parsed from the mode switch (case"<family>":)
//   - ${tool} interpolations:     resolved from the binary's tool-name bindings
//   - merge/oracle helpers:       traced from the referenced builder outward
// A single rotated identifier no longer silently disables the check.

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import zlib from "node:zlib";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const args = parseArgs(process.argv.slice(2));
const sourcePath = path.resolve(repoRoot, args.source ?? "internal/api/modules/amp/neo_runtime.go");
const binaryPath = path.resolve(args.binary ?? path.join(process.env.HOME ?? "", ".amp", "bin", "amp"));

const source = fs.readFileSync(sourcePath, "utf8");
const binary = fs.readFileSync(binaryPath).toString("latin1");

const promptBlobs = {
  aggman: "neoPromptFamilyAggManGzip",
  rush: "neoPromptFamilyRushGzip",
  gpt: "neoPromptFamilyGPTGzip",
  "gpt-5-codex": "neoPromptFamilyGPT5CodexGzip",
  "deep-gpt5.4": "neoPromptFamilyDeepGPT54Gzip",
  deep: "neoPromptFamilyDeepGzip",
  xai: "neoPromptFamilyXAIGzip",
  kimi: "neoPromptFamilyKimiGzip",
  default: "neoPromptFamilyDefaultGzip",
  gemini: "neoPromptFamilyGeminiGzip",
};
const gitCommitMultilinePromptLine = "When passing a multi-line body to `git commit -m` in a Bash command, put real line breaks in the quoted argument; do not write literal `\\n` escape sequences.";

// Tool names are stable strings; only the minified vars bound to them rotate.
// When a var name collides across bundle chunks, prefer the binding whose value
// is a real tool name over an unrelated reuse of the same identifier.
const knownToolNames = new Set([
  "Bash", "Read", "edit_file", "create_file", "AGENTS.md", "oracle", "get_diagnostics",
  "Grep", "finder", "glob", "web_search", "read_web_page", "librarian", "shell_command",
  "apply_patch", "Task", "find_thread", "read_thread", "create_project", "create_thread",
  "send_message_to_thread", "send_message_to_aggman", "archive_thread", "archive_threads",
  "unarchive_thread", "docs_list", "docs_read", "docs_write", "render_agg_man",
  "github_repo_ci_status", "slack_read", "slack_write", "chart", "todo_write",
  "run_terminal_command", "read_file",
]);

const checks = [
  ["aggman", () => evalAggManPrompt(), () => runtimePrompt("aggman")],
  ["rush", () => evalSimplePrompt("rush"), () => runtimePrompt("rush")],
  ["gpt", () => evalSimplePrompt("gpt"), () => runtimePrompt("gpt")],
  ["gpt-5-codex", () => evalSimplePrompt("gpt-5-codex"), () => runtimePrompt("gpt-5-codex")],
  ["deep-gpt5.4", () => evalSimplePrompt("deep-gpt5.4"), () => runtimePrompt("deep-gpt5.4")],
  ["deep", () => evalSimplePrompt("deep"), () => runtimePrompt("deep")],
  ["xai", () => evalXaiPrompt(), () => runtimePrompt("xai")],
  ["kimi", () => evalSimplePrompt("kimi"), () => runtimePrompt("kimi")],
  ["default", () => evalSimplePrompt("default"), () => runtimePrompt("default")],
  ["gemini base", () => evalGeminiPrompt({ enableOracle: false, enableDiagnostics: false }), () => runtimeGeminiPrompt({ enableOracle: false, enableDiagnostics: false })],
  ["gemini oracle", () => evalGeminiPrompt({ enableOracle: true, enableDiagnostics: false }), () => runtimeGeminiPrompt({ enableOracle: true, enableDiagnostics: false })],
  ["gemini diagnostics", () => evalGeminiPrompt({ enableOracle: false, enableDiagnostics: true }), () => runtimeGeminiPrompt({ enableOracle: false, enableDiagnostics: true })],
  ["gemini oracle diagnostics", () => evalGeminiPrompt({ enableOracle: true, enableDiagnostics: true }), () => runtimeGeminiPrompt({ enableOracle: true, enableDiagnostics: true })],
];

function parseArgs(values) {
  const parsed = {};
  for (let i = 0; i < values.length; i++) {
    const arg = values[i];
    if (arg === "--binary" || arg === "--source") {
      const value = values[++i];
      if (!value) throw new Error(`${arg} requires a value`);
      parsed[arg.slice(2)] = value;
      continue;
    }
    if (arg === "-h" || arg === "--help") {
      console.log("Usage: bun dev/amp-prompt-family-audit.mjs [--binary ~/.amp/bin/amp] [--source internal/api/modules/amp/neo_runtime.go]");
      process.exit(0);
    }
    throw new Error(`unknown argument: ${arg}`);
  }
  return parsed;
}

// ---------- minified-JS scanners (string/template/brace/bracket aware) ----------

function scanString(src, i) {
  const quote = src[i];
  i++;
  while (i < src.length) {
    const c = src[i];
    if (c === "\\") { i += 2; continue; }
    if (c === quote) return i + 1;
    i++;
  }
  throw new Error("unterminated string");
}

function scanTemplate(src, i) {
  i++;
  while (i < src.length) {
    const c = src[i];
    if (c === "\\") { i += 2; continue; }
    if (c === "`") return i + 1;
    if (c === "$" && src[i + 1] === "{") { i = scanBraces(src, i + 2); continue; }
    i++;
  }
  throw new Error("unterminated template");
}

function scanBraces(src, i) {
  let depth = 1;
  while (i < src.length) {
    const c = src[i];
    if (c === "\\") { i += 2; continue; }
    if (c === '"' || c === "'") { i = scanString(src, i); continue; }
    if (c === "`") { i = scanTemplate(src, i); continue; }
    if (c === "{") { depth++; i++; continue; }
    if (c === "}") { depth--; if (depth === 0) return i + 1; i++; continue; }
    i++;
  }
  throw new Error("unterminated braces");
}

function scanBrackets(src, start) {
  const open = src.indexOf("[", start);
  let depth = 0;
  for (let i = open; i < src.length; i++) {
    const c = src[i];
    if (c === "\\") { i++; continue; }
    if (c === '"' || c === "'") { i = scanString(src, i) - 1; continue; }
    if (c === "`") { i = scanTemplate(src, i) - 1; continue; }
    if (c === "[") depth++;
    else if (c === "]") { depth--; if (depth === 0) return src.slice(start, i + 1); }
  }
  throw new Error("unterminated array");
}

// ---------- binary-derived identifiers ----------

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function switchBuilder(family) {
  const re = new RegExp(`case"${escapeRegExp(family)}":[A-Za-z0-9_$]*=([A-Za-z0-9_$]+)\\(`);
  const match = binary.match(re);
  if (!match) throw new Error(`missing prompt switch case for ${family}`);
  return match[1];
}

function binaryEmbedsFamilyPrompts() {
  // The per-family prompt builders were dispatched by `switch(p){case"<family>":...}`
  // in the CLI binary. As of Amp g2007df the per-model/mode system prompts were moved
  // server-side (the actor that assembles them is server-side), so neither the builders
  // nor the prose remain embedded. Detect that instead of failing extraction, and fall
  // back to verifying our frozen local blobs still decode.
  for (const family of Object.keys(promptBlobs)) {
    const escaped = family.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    if (new RegExp(`case"${escaped}":[A-Za-z0-9_$]*=([A-Za-z0-9_$]+)\\(`).test(binary)) {
      return true;
    }
  }
  return false;
}

if (!binaryEmbedsFamilyPrompts()) {
  console.log("Amp no longer embeds per-family system prompts in the CLI binary.");
  console.log("They moved server-side as of g2007df, so a byte-comparison against the binary");
  console.log("is no longer possible. Our neoPromptFamily*Gzip blobs are now the frozen local");
  console.log("source of truth for local inference; verifying they still decode:\n");
  let blobFailures = 0;
  for (const [family, constName] of Object.entries(promptBlobs)) {
    let prompt, error = null;
    try {
      prompt = decodePromptBlob(constName);
    } catch (e) {
      error = e;
    }
    if (error || !prompt || prompt.trim().length < 100) {
      console.error(`ERR  ${family.padEnd(14)} ${error ? error.message : "blob decoded empty or too short"}`);
      blobFailures++;
      continue;
    }
    const digest = crypto.createHash("sha256").update(prompt).digest("hex").slice(0, 12);
    console.log(`OK   ${family.padEnd(14)} len=${String(prompt.length).padStart(5)} sha=${digest}`);
  }
  if (blobFailures > 0) {
    console.error(`\nblob integrity check failed: ${blobFailures} blob(s)`);
    process.exit(1);
  }
  console.log("\nAll local prompt blobs intact. Amp's server-side prompts can no longer be");
  console.log("diffed from the binary; track them via request capture if exact parity is needed.");
  process.exit(0);
}

const builders = {};
for (const family of ["aggman", "rush", "gpt", "gpt-5-codex", "deep-gpt5.4", "deep", "xai", "kimi"]) {
  builders[family] = switchBuilder(family);
}
builders.gemini = matchOrThrow(/case"gemini":[A-Za-z0-9_$]*=([A-Za-z0-9_$]+)\(\{enableOracle/, "gemini builder");
builders.default = matchOrThrow(/default:[A-Za-z0-9_$]*=([A-Za-z0-9_$]+)\(\)[;}]/, "default builder");

function matchOrThrow(re, label) {
  const match = binary.match(re);
  if (!match) throw new Error(`missing ${label}`);
  return match[1];
}

function decodeJSString(literal) {
  return JSON.parse('"' + literal.replace(/"/g, '\\"') + '"');
}

// Resolve a ${ident} placeholder to its tool-name string. A minified name can be
// reused for different values across chunks, so prefer a real tool name, then the
// binding physically nearest the prompt that referenced it.
function resolveToken(id, nearOffset) {
  const re = new RegExp(`(?:^|[^A-Za-z0-9_$])${escapeRegExp(id)}="((?:\\\\.|[^"\\\\])*)"`, "g");
  const candidates = [];
  for (const match of binary.matchAll(re)) {
    const at = match.index + match[0].length - match[1].length - 1;
    candidates.push({ value: match[1], at });
  }
  if (candidates.length === 0) return null;
  const known = candidates.filter((c) => knownToolNames.has(decodeJSString(c.value)));
  const pool = known.length ? known : candidates;
  pool.sort((a, b) => Math.abs(a.at - nearOffset) - Math.abs(b.at - nearOffset));
  return pool[0].value;
}

function toolStubs(body, nearOffset, skip = new Set()) {
  const ids = [...new Set([...body.matchAll(/\$\{([A-Za-z0-9_$]+)/g)].map((m) => m[1]))].filter((id) => !skip.has(id));
  let out = "";
  for (const id of ids) {
    const value = resolveToken(id, nearOffset);
    if (value != null) out += `var ${id}=${JSON.stringify(decodeJSString(value))};`;
  }
  return out;
}

// ---------- binary prompt evaluation ----------

function functionStart(id) {
  const start = binary.indexOf(`function ${id}(`);
  if (start < 0) throw new Error(`missing binary function ${id}`);
  return start;
}

function functionText(id) {
  const start = functionStart(id);
  const open = binary.indexOf("{", start);
  const end = scanBraces(binary, open + 1);
  return binary.slice(start, end);
}

function templateReturningBody(id) {
  const text = functionText(id);
  const tick = text.indexOf("`");
  const last = text.lastIndexOf("`");
  return text.slice(tick + 1, last);
}

function evalSimplePrompt(family) {
  const id = builders[family];
  const at = functionStart(id);
  const stubs = toolStubs(templateReturningBody(id), at);
  return Function(`${stubs}${functionText(id)}; return ${id};`)()();
}

function evalXaiPrompt() {
  const id = builders.xai;
  const at = functionStart(id);
  const stubs = toolStubs(templateReturningBody(id), at, new Set(["T"]));
  return Function(`${stubs}${functionText(id)}; return ${id};`)()({});
}

// Agg Man interpolates the canonical merge prompt, built by a helper that spreads
// a constant array. Derive the helper/array from the placeholder that does not
// resolve to a tool-name string, so the merge identifiers can rotate freely.
function evalAggManPrompt() {
  const id = builders.aggman;
  const at = functionStart(id);
  const body = templateReturningBody(id);
  const bodyTokens = [...new Set([...body.matchAll(/\$\{([A-Za-z0-9_$]+)/g)].map((m) => m[1]))];
  const skip = new Set();
  let setup = "";
  for (const token of bodyTokens) {
    if (resolveToken(token, at) != null) continue;
    const assignment = nearestCallAssignment(token, at);
    if (!assignment) continue;
    skip.add(token);
    const calleeText = functionText(assignment.callee);
    const calleeStart = functionStart(assignment.callee);
    for (const arrayId of new Set([...calleeText.matchAll(/\.\.\.([A-Za-z0-9_$]+)/g)].map((m) => m[1]))) {
      setup += `var ${nearestArray(arrayId, calleeStart)};`;
      skip.add(arrayId);
    }
    setup += `${calleeText};var ${token}=${assignment.callee}(${assignment.argText});`;
  }
  const stubs = toolStubs(body, at, skip);
  return Function(`${stubs}${setup}${functionText(id)}; return ${id};`)()();
}

function nearestCallAssignment(id, nearOffset) {
  const re = new RegExp(`(?:^|[^A-Za-z0-9_$])${escapeRegExp(id)}=([A-Za-z0-9_$]+)\\(`, "g");
  let best = null;
  for (const match of binary.matchAll(re)) {
    const callOpen = match.index + match[0].length - 1;
    const argText = binary.slice(callOpen + 1, matchingParen(binary, callOpen));
    const dist = Math.abs(match.index - nearOffset);
    if (!best || dist < best.dist) best = { callee: match[1], argText, dist };
  }
  return best;
}

function matchingParen(src, open) {
  let depth = 0;
  for (let i = open; i < src.length; i++) {
    const c = src[i];
    if (c === "\\") { i++; continue; }
    if (c === '"' || c === "'") { i = scanString(src, i) - 1; continue; }
    if (c === "`") { i = scanTemplate(src, i) - 1; continue; }
    if (c === "(") depth++;
    else if (c === ")") { depth--; if (depth === 0) return i; }
  }
  throw new Error("unterminated call");
}

function nearestArray(id, nearOffset) {
  const re = new RegExp(`(?:^|[^A-Za-z0-9_$])${escapeRegExp(id)}=\\[`, "g");
  let best = null;
  for (const match of binary.matchAll(re)) {
    const start = match.index + match[0].length - (`${id}=[`).length;
    const dist = Math.abs(start - nearOffset);
    if (!best || dist < best.dist) best = { start, dist };
  }
  if (!best) throw new Error(`missing binary array ${id}`);
  return scanBrackets(binary, best.start);
}

function evalGeminiPrompt(flags) {
  const id = builders.gemini;
  const search = binary.search(new RegExp(`(?:^|[^A-Za-z0-9_$])${escapeRegExp(id)}=\\(`));
  const idStart = /[A-Za-z0-9_$]/.test(binary[search]) ? search : search + 1;
  const eq = binary.indexOf("=", idStart);
  const tick = binary.indexOf("`", eq);
  const end = scanTemplate(binary, tick);
  const arrowText = binary.slice(idStart, end);
  const body = binary.slice(tick + 1, end - 1);
  const stubs = toolStubs(body, idStart, new Set(["T", "R"]));
  return Function(`${stubs}var ${arrowText}; return ${id};`)()(flags);
}

// ---------- runtime side (our shipped blobs) ----------

function goStringConcat(name) {
  const match = source.match(new RegExp(`\\b${name}\\s*=\\s*((?:"(?:\\\\.|[^"\\\\])*"\\s*\\+?\\s*)+)`, "m"));
  if (!match) throw new Error(`missing Go string constant ${name}`);
  return [...match[1].matchAll(/"(?:\\.|[^"\\])*"/g)].map((part) => JSON.parse(part[0])).join("");
}

function goSimpleString(name) {
  const match = source.match(new RegExp(`\\b${name}\\s*=\\s*("(?:\\\\.|[^"\\\\])*")`));
  if (!match) throw new Error(`missing Go string constant ${name}`);
  return JSON.parse(match[1]);
}

function goExpressionBetween(name, nextName) {
  const match = new RegExp(`\\b${name}\\b\\s*=`).exec(source);
  if (!match) throw new Error(`missing Go expression ${name}`);
  const equals = source.indexOf("=", match.index);
  const end = source.indexOf(nextName, equals);
  if (end < 0) throw new Error(`missing Go expression end marker ${nextName}`);
  return source.slice(equals + 1, end);
}

function concatGoStringLiterals(expression) {
  let output = "";
  for (let i = 0; i < expression.length; i++) {
    if (expression[i] === "`") {
      const end = expression.indexOf("`", i + 1);
      if (end < 0) throw new Error("unterminated Go raw string");
      output += expression.slice(i + 1, end);
      i = end;
      continue;
    }
    if (expression[i] === "\"") {
      let end = i + 1;
      let escaped = false;
      for (; end < expression.length; end++) {
        if (escaped) { escaped = false; continue; }
        if (expression[end] === "\\") { escaped = true; continue; }
        if (expression[end] === "\"") break;
      }
      output += JSON.parse(expression.slice(i, end + 1));
      i = end;
    }
  }
  return output;
}

function decodePromptBlob(name) {
  return zlib.gunzipSync(Buffer.from(goStringConcat(name), "base64")).toString("utf8");
}

function insertAfter(prompt, needle, line) {
  if (!prompt.includes(needle)) return prompt;
  return prompt.replace(needle, `${needle}\n${line}`);
}

function applyBinaryPromptUpdates(name, prompt) {
  if (!prompt || prompt.includes(gitCommitMultilinePromptLine)) return prompt;
  if (name === "aggman") {
    return prompt.replace("Use archive_threads, archive_thread, and unarchive_thread", "Use archive_thread, archive_threads, and unarchive_thread");
  }
  if (name === "deep") {
    return insertAfter(prompt, "Don't use it for simple local file reads.", gitCommitMultilinePromptLine);
  }
  if (name === "deep-gpt5.4") {
    return insertAfter(prompt, "prefer official docs first, then source.", `- ${gitCommitMultilinePromptLine}`);
  }
  return prompt;
}

function runtimePrompt(family) {
  return applyBinaryPromptUpdates(family, decodePromptBlob(promptBlobs[family]));
}

function runtimeGeminiPrompt({ enableOracle = false, enableDiagnostics = false }) {
  let prompt = decodePromptBlob(promptBlobs.gemini);
  if (enableOracle) {
    const guidanceLine = goSimpleString("neoGeminiOracleGuidanceLine");
    const guidanceNeedle = goSimpleString("neoGeminiOracleGuidanceNeedle");
    const oracleSectionNeedle = goSimpleString("neoGeminiOracleSectionNeedle");
    const oracleSection = concatGoStringLiterals(goExpressionBetween("neoGeminiOracleSection", "neoGitCommitMultilinePromptLine"));
    prompt = prompt.replace(guidanceNeedle, `\n${guidanceLine}\n- Use search tools like finder`);
    prompt = prompt.replace(oracleSectionNeedle, `\n\n${oracleSection}\n\n\n# Conventions & Rules`);
  }
  if (enableDiagnostics) {
    prompt = prompt.replace(goSimpleString("neoGeminiDiagnosticsNeedle"), goSimpleString("neoGeminiDiagnosticsWithTool"));
  }
  return prompt;
}

function firstDifference(left, right) {
  let index = 0;
  while (index < left.length && index < right.length && left[index] === right[index]) index++;
  return index;
}

let failures = 0;
for (const [name, binFn, runtimeFn] of checks) {
  let binaryPrompt, runtime, error = null;
  try {
    binaryPrompt = binFn();
    runtime = runtimeFn();
  } catch (e) {
    error = e;
  }
  if (error) {
    console.error(`ERR  ${name.padEnd(27)} ${error.message}`);
    failures++;
    continue;
  }
  const ok = binaryPrompt === runtime;
  const digest = crypto.createHash("sha256").update(binaryPrompt).digest("hex").slice(0, 12);
  console.log(`${ok ? "PASS" : "FAIL"} ${name.padEnd(27)} len=${String(binaryPrompt.length).padStart(5)} sha=${digest}`);
  if (!ok) {
    failures++;
    const index = firstDifference(binaryPrompt, runtime);
    console.log(`  first difference: ${index}`);
    console.log(`  binary : ${JSON.stringify(binaryPrompt.slice(index, index + 220))}`);
    console.log(`  runtime: ${JSON.stringify(runtime.slice(index, index + 220))}`);
  }
}

if (failures > 0) {
  console.error(`prompt family audit failed: ${failures} mismatch(es)`);
  process.exit(1);
}
