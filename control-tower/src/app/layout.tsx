import type { Metadata } from 'next';
import './globals.css';
import DemoBanner from '@/components/DemoBanner';
import { DEMO_MODE } from '@/lib/demo';

export const metadata: Metadata = {
  title: 'DRADIS Control Tower',
  description: 'Polymarket strategy orchestration dashboard',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body className={DEMO_MODE ? 'pb-12' : undefined}>
        {children}
        <DemoBanner />
      </body>
    </html>
  );
}

