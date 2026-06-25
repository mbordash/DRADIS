// Read-only demo mode flag.
//
// When `NEXT_PUBLIC_DEMO_MODE=true` at build time, the Control Tower renders a
// persistent "read-only demo" banner and disables every mutating control
// (viper toggles, config edits, RTB / manual-exit). This is the UI half of the
// demo; the backend independently rejects all writes when `DRADIS_READ_ONLY=true`,
// so the demo is safe even if a control is missed or the API is hit directly.
//
// NEXT_PUBLIC_* vars are inlined at build time, so this works in both server and
// client components.
export const DEMO_MODE = process.env.NEXT_PUBLIC_DEMO_MODE === 'true';

/** Public repo URL surfaced in the demo banner. */
export const REPO_URL = 'https://github.com/mbordash/DRADIS';
