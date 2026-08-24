#!/usr/bin/env node
/**
 * Sort TypeScript imports by descending length — single-line statements first
 * (longest to shortest), then multi-line ones ordered by the length of their
 * closing line. Produces the ragged-left-edge block this codebase uses.
 *
 * Zero-dependency: no commander, so it runs without an install step.
 *
 *   node scripts/sort-imports.mjs                 # cwd, in place
 *   node scripts/sort-imports.mjs --dir src       # a directory
 *   node scripts/sort-imports.mjs --file a.ts     # one file
 *   node scripts/sort-imports.mjs --check         # exit 1 if anything is unsorted
 *   node scripts/sort-imports.mjs --dry-run       # print what would change
 */
import fs from "node:fs";
import path from "node:path";

const SKIP_DIRS = new Set([
  "node_modules",
  "build",
  "dist",
  ".git",
  "coverage",
]);

function parseImports(content) {
  const lines = content.split("\n");
  const imports = [];
  const importLineIndices = new Set();

  let shebang;
  if (lines[0]?.startsWith("#!")) {
    shebang = lines[0];
    importLineIndices.add(0);
  }

  let i = shebang ? 1 : 0;
  while (i < lines.length) {
    const line = lines[i];

    if (line.trim().startsWith("import ")) {
      const importLines = [line];
      let j = i;
      importLineIndices.add(i);

      while (j < lines.length && !lines[j].includes(";")) {
        j++;
        if (j < lines.length) {
          importLines.push(lines[j]);
          importLineIndices.add(j);
        }
      }

      const original = importLines.join("\n");
      const isSingleLine = importLines.length === 1;
      const sortKey = isSingleLine
        ? original.length
        : // Multi-line statements sort among themselves, after every
          // single-line one, keyed on their closing line.
          100000 + importLines[importLines.length - 1].length;

      imports.push({ original, isSingleLine, sortKey });
      i = j + 1;
    } else {
      i++;
    }
  }

  const nonImportLines = lines.filter((_, idx) => !importLineIndices.has(idx));
  while (nonImportLines.length > 0 && nonImportLines[0].trim() === "") {
    nonImportLines.shift();
  }

  return { imports, restOfFile: nonImportLines.join("\n"), shebang };
}

function sortImports(imports) {
  const single = imports
    .filter((imp) => imp.isSingleLine)
    .sort((a, b) => b.sortKey - a.sortKey);
  const multi = imports
    .filter((imp) => !imp.isSingleLine)
    .sort((a, b) => b.sortKey - a.sortKey);
  return [...single, ...multi].map((imp) => imp.original);
}

function sortedContent(content) {
  const { imports, restOfFile, shebang } = parseImports(content);
  if (imports.length === 0) return null;
  const prefix = shebang ? shebang + "\n" : "";
  return prefix + sortImports(imports).join("\n") + "\n\n" + restOfFile;
}

function findTypeScriptFiles(dir) {
  const files = [];
  const walk = (current) => {
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) {
        if (!SKIP_DIRS.has(entry.name)) walk(full);
      } else if (/\.tsx?$/.test(entry.name)) {
        files.push(full);
      }
    }
  };
  walk(dir);
  return files;
}

const args = process.argv.slice(2);
const has = (flag) => args.includes(flag);
const valueOf = (flag) => {
  const i = args.indexOf(flag);
  return i === -1 ? undefined : args[i + 1];
};

const check = has("--check");
const dryRun = has("--dry-run");
const fileArg = valueOf("--file");
const dirArg = valueOf("--dir");

let targets;
if (fileArg) {
  const abs = path.resolve(fileArg);
  if (!fs.existsSync(abs)) {
    console.error(`File not found: ${abs}`);
    process.exit(1);
  }
  targets = [abs];
} else {
  const abs = path.resolve(dirArg ?? process.cwd());
  if (!fs.existsSync(abs)) {
    console.error(`Directory not found: ${abs}`);
    process.exit(1);
  }
  targets = findTypeScriptFiles(abs);
}

const unsorted = [];
let changed = 0;

for (const file of targets) {
  const content = fs.readFileSync(file, "utf8");
  const next = sortedContent(content);
  if (next === null || next === content) continue;

  const rel = path.relative(process.cwd(), file);
  if (check) {
    unsorted.push(rel);
  } else if (dryRun) {
    console.log(`would sort ${rel}`);
    changed++;
  } else {
    fs.writeFileSync(file, next);
    console.log(`✓ ${rel}`);
    changed++;
  }
}

if (check) {
  if (unsorted.length > 0) {
    console.error(
      `${unsorted.length} file(s) have unsorted imports:\n` +
        unsorted.map((f) => `  ${f}`).join("\n") +
        `\n\nRun \`npm run sort-imports\` to fix.`
    );
    process.exit(1);
  }
  console.log(`All ${targets.length} file(s) have sorted imports.`);
} else {
  console.log(
    `${changed} of ${targets.length} file(s) ${dryRun ? "would be" : ""} updated.`
  );
}
