'use client';

import { DEMO_MODE, REPO_URL } from '@/lib/demo';

/**
 * Persistent read-only demo banner.
 *
 * Renders nothing unless `NEXT_PUBLIC_DEMO_MODE=true`. When active it pins a
 * thin bar to the bottom of the viewport explaining the site is a live but
 * read-only demo and linking to the repo so visitors can deploy their own.
 *
 * The banner is `position: fixed`; `globals`/`layout` add matching bottom
 * padding so it never covers dashboard content.
 */
export default function DemoBanner() {
  if (!DEMO_MODE) return null;

  return (
    <div
      role="note"
      aria-label="Read-only demo notice"
      className="fixed inset-x-0 bottom-0 z-[100] border-t border-cyan-500/30 bg-[#0b0b14]/95 backdrop-blur supports-[backdrop-filter]:bg-[#0b0b14]/80"
    >
      <div className="mx-auto flex max-w-7xl items-center justify-center gap-2 px-4 py-2 text-center text-xs font-mono text-gray-300 sm:text-sm">
        <span className="hidden sm:inline" aria-hidden>🔭</span>
        <span>
          This is a <span className="font-semibold text-cyan-300">live, read-only demo</span> of DRADIS — raptor telemetry
          streams in real time, but no controls are active.
        </span>
        <a
          href={REPO_URL}
          target="_blank"
          rel="noopener noreferrer"
          className="ml-1 inline-flex items-center gap-1 rounded border border-cyan-500/40 bg-cyan-500/10 px-2 py-0.5 font-semibold text-cyan-200 transition-colors hover:bg-cyan-500/20"
        >
          Deploy your own ↗
        </a>
      </div>
    </div>
  );
}
