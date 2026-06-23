'use client';

import { Component, type ReactNode } from 'react';

interface Props {
  children: ReactNode;
  /** Short label shown in the fallback card, e.g. "Telemetry". */
  label?: string;
}

interface State {
  error: Error | null;
}

/**
 * Localized error boundary. Prevents a render-time exception in one view (e.g. a
 * formatter hitting unexpected data) from white-screening the entire dashboard.
 * The failed subtree degrades to an inline card; the rest of the app keeps working.
 */
export default class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: unknown) {
    // Surface to the console for debugging; no external reporting.
    console.error(`[${this.props.label ?? 'view'}] render error:`, error, info);
  }

  reset = () => this.setState({ error: null });

  render() {
    if (this.state.error) {
      return (
        <div className="card px-4 py-3 border border-red-500/30 bg-red-500/5 text-red-300 text-xs font-mono space-y-2">
          <p className="font-semibold">
            {this.props.label ?? 'This view'} failed to render.
          </p>
          <p className="text-red-400/80 break-all">{this.state.error.message}</p>
          <button
            onClick={this.reset}
            className="px-3 py-1 rounded border border-red-500/40 text-red-200 hover:bg-red-500/10 transition-colors"
          >
            Retry
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

