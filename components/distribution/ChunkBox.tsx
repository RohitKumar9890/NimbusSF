'use client';

import { cn } from '@/lib/utils';
import type { ChunkInfo } from '@/lib/types';

interface ChunkBoxProps {
  chunk: ChunkInfo;
  nodeName: string;
  isNodeOnline: boolean;
  index: number;
}

const nodeColors: Record<string, { bg: string; text: string }> = {
  '1': { bg: 'bg-chart-1', text: 'text-chart-1' },
  '2': { bg: 'bg-chart-2', text: 'text-chart-2' },
  '3': { bg: 'bg-chart-3', text: 'text-chart-3' },
};

export function ChunkBox({ chunk, nodeName, isNodeOnline, index }: ChunkBoxProps) {
  const colors = nodeColors[chunk.nodeId] || { bg: 'bg-muted', text: 'text-muted-foreground' };

  const formatSize = (bytes: number): string => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  return (
    <div
      className={cn(
        'relative p-3 rounded-lg border transition-all',
        isNodeOnline
          ? 'border-border bg-card hover:border-primary/50'
          : 'border-status-offline/30 bg-status-offline/5 opacity-60'
      )}
    >
      {/* Chunk Index Badge */}
      <div
        className={cn(
          'absolute -top-2 -left-2 w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold',
          colors.bg
        )}
      >
        <span className="text-primary-foreground">{index + 1}</span>
      </div>

      {/* Content */}
      <div className="flex flex-col gap-2 pt-1">
        <div className="flex items-center justify-between">
          <span className="text-sm font-medium">Chunk {index + 1}</span>
          <span className={cn('text-xs', colors.text)}>{nodeName}</span>
        </div>

        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <span>Size</span>
          <span>{formatSize(chunk.size)}</span>
        </div>

        {/* Node Status Indicator */}
        <div className="flex items-center gap-2 mt-1">
          <div
            className={cn(
              'w-2 h-2 rounded-full',
              isNodeOnline ? 'bg-status-online' : 'bg-status-offline'
            )}
          />
          <span className="text-xs text-muted-foreground">
            {isNodeOnline ? 'Available' : 'Unavailable'}
          </span>
        </div>
      </div>
    </div>
  );
}
