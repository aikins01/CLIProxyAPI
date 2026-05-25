import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';
import { fileURLToPath } from 'node:url';

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
      '/api': {
        target: 'http://127.0.0.1:8317',
        changeOrigin: true
      },
      '/threads': {
        target: 'http://127.0.0.1:8317',
        changeOrigin: true
      },
      '/gateway': {
        target: 'http://127.0.0.1:8317',
        changeOrigin: true,
        ws: true
      },
      '/actors': {
        target: 'http://127.0.0.1:8317',
        changeOrigin: true,
        ws: true
      }
    }
  }
});
