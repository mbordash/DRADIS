import type { Config } from 'tailwindcss';

const config: Config = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      fontFamily: {
        mono: ['JetBrains Mono', 'Fira Code', 'ui-monospace', 'monospace'],
      },
      colors: {
        surface: {
          DEFAULT: '#0f0f1a',
          card:    '#13131f',
          border:  '#1e1e32',
          hover:   '#1a1a2e',
        },
      },
    },
  },
  plugins: [],
};

export default config;

