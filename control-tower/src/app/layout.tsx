import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'DRADIS Control Tower',
  description: 'Polymarket strategy orchestration dashboard',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body>{children}</body>
    </html>
  );
}

