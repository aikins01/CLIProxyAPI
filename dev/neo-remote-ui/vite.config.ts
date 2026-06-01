import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';
import { fileURLToPath } from 'node:url';

const runtimeUpstream = process.env.CLIPROXY_UPSTREAM || 'http://127.0.0.1:8317';
const runtimeProxy = (ws = false) => ({
  target: runtimeUpstream,
  changeOrigin: true,
  ...(ws ? { ws: true } : {})
});

export default defineConfig({
  plugins: [sveltekit()],
  resolve: {
    alias: {
      '@pierre/diffs-web-components': fileURLToPath(
        new URL('./node_modules/@pierre/diffs/dist/components/web-components.js', import.meta.url)
      )
    }
  },
  build: {
    chunkSizeWarningLimit: 1000
  },
  server: {
    port: 5177,
    proxy: {
      '/api': runtimeProxy(),
      '/threads': runtimeProxy(),
      '/gateway': runtimeProxy(true),
      '/actors': runtimeProxy(true),
      '/metadata': runtimeProxy()
    }
  }
});
