<script lang="ts">
  import { onMount } from 'svelte';
  import type { FileDiffMetadata } from '@pierre/diffs';

  type ThemeMode = 'system' | 'dark' | 'light';
  type RenderedFile = {
    name: string;
    additions: number;
    deletions: number;
  };

  let { patch = '', mode = 'system' }: { patch?: string; mode?: ThemeMode } = $props();

  let root: HTMLDivElement;
  let error = $state('');
  let files = $state<RenderedFile[]>([]);
  let renderToken = 0;
  let cleanups: Array<{ cleanUp: () => void }> = [];

  onMount(() => {
    void render();
    return () => cleanup();
  });

  $effect(() => {
    patch;
    mode;
    if (root) {
      void render();
    }
  });

  async function render() {
    const token = ++renderToken;
    error = '';
    files = [];
    cleanup();
    root?.replaceChildren();
    if (!patch.trim() || !root) return;

    try {
      await import('@pierre/diffs-web-components');
      const { FileDiff, parsePatchFiles } = await import('@pierre/diffs');
      if (token !== renderToken) return;

      const parsed = parsePatchFiles(patch, `neo-${hashPatch(patch)}`, true);
      const parsedFiles = parsed.flatMap((item) => item.files).filter((file) => file.hunks.length > 0);
      files = parsedFiles.map(fileSummary);

      if (parsedFiles.length === 0) {
        error = 'No renderable diff hunks found.';
        return;
      }

      for (const fileDiff of parsedFiles) {
        const container = document.createElement('diffs-container');
        container.className = 'pierre-diff';
        root.appendChild(container);

        const instance = new FileDiff({
          theme: { dark: 'pierre-dark-soft', light: 'pierre-light-soft' },
          themeType: resolvedThemeType(mode),
          diffStyle: 'unified',
          diffIndicators: 'bars',
          hunkSeparators: 'line-info-basic',
          collapsedContextThreshold: 4,
          expansionLineCount: 12,
          overflow: 'wrap',
          lineDiffType: 'word',
          disableFileHeader: false,
          stickyHeader: false,
          unsafeCSS: `
            :host {
              --diffs-font-size: 12px;
              --diffs-line-height: 19px;
              --diffs-font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
              --diffs-header-font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
              --diffs-dark-bg: #0b0e0c;
              --diffs-light-bg: #fffdf8;
              --diffs-dark: #f4f6f0;
              --diffs-light: #171915;
              --diffs-addition-color: #49c172;
              --diffs-deletion-color: #ee7168;
            }
          `
        });
        instance.render({ fileDiff, fileContainer: container });
        cleanups.push(instance);
      }
    } catch (cause) {
      error = cause instanceof Error ? cause.message : String(cause);
    }
  }

  function cleanup() {
    for (const instance of cleanups) {
      instance.cleanUp();
    }
    cleanups = [];
  }

  function fileSummary(file: FileDiffMetadata): RenderedFile {
    const additions = file.hunks.reduce((sum, hunk) => sum + hunk.additionLines, 0);
    const deletions = file.hunks.reduce((sum, hunk) => sum + hunk.deletionLines, 0);
    return { name: file.name, additions, deletions };
  }

  function resolvedThemeType(next: ThemeMode) {
    if (next === 'light') return 'light';
    if (next === 'dark') return 'dark';
    return matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
  }

  function hashPatch(value: string) {
    let hash = 0;
    for (let index = 0; index < value.length; index += 1) {
      hash = (hash << 5) - hash + value.charCodeAt(index);
      hash |= 0;
    }
    return Math.abs(hash).toString(36);
  }
</script>

<div class="patch-shell">
  {#if files.length > 0}
    <div class="patch-summary" aria-label="Changed files">
      {#each files as file}
        <span>
          {file.name}
          <b>+{file.additions}</b>
          <i>-{file.deletions}</i>
        </span>
      {/each}
    </div>
  {/if}

  <div class="patch-root" bind:this={root}></div>

  {#if error}
    <pre class="patch-fallback">{patch}</pre>
  {/if}
</div>

<style>
  .patch-shell {
    display: grid;
    gap: 8px;
  }

  .patch-summary {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    color: var(--neo-muted);
    font-size: 12px;
  }

  .patch-summary span {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    max-width: 100%;
    border: 1px solid var(--neo-border);
    background: var(--neo-field);
    padding: 4px 7px;
  }

  .patch-summary b,
  .patch-summary i {
    font-style: normal;
    font-weight: 700;
  }

  .patch-summary b {
    color: var(--neo-success);
  }

  .patch-summary i {
    color: var(--neo-danger);
  }

  .patch-root {
    display: grid;
    gap: 10px;
    min-width: 0;
  }

  :global(.pierre-diff) {
    display: block;
    overflow: hidden;
    border: 1px solid var(--neo-border);
    background: var(--neo-panel);
  }

  .patch-fallback {
    overflow: auto;
    max-height: 420px;
    margin: 0;
    border: 1px solid var(--neo-border);
    background: var(--neo-field);
    color: var(--neo-muted);
    padding: 10px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 12px;
    line-height: 1.55;
  }
</style>
