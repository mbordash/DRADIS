// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS Control Tower — operator dashboard for the DRADIS trading engine.
// Copyright (C) 2026 Michael Bordash
//
// This file is part of DRADIS. DRADIS is free software: you can redistribute it
// and/or modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.
//
// DRADIS is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
// A PARTICULAR PURPOSE. See the GNU Affero General Public License for details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

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

