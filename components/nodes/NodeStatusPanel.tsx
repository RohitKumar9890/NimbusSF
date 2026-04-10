'use client';

import { cn } from '@/lib/utils';
import type { NodeStatus } from '@/lib/types';

interface NodeStatusPanelProps {
  nodes: NodeStatus[];
  compact?: boolean;
}

export function NodeStatusPanel({ nodes, compact = false }: NodeStatusPanelProps) {
  if (compact) {
    return (
      <div className="flex gap-3">
        {nodes.map((node) => (
          <NodeBox key={node.id} node={node} compact />
        ))}
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
      {nodes.map((node) => (
        <NodeBox key={node.id} node={node} />
      ))}
    </div>
  );
}

interface NodeBoxProps {
  node: NodeStatus;
  compact?: boolean;
}

export function NodeBox({ node, compact = false }: NodeBoxProps) {
  const storagePercentage = (node.storage.used / node.storage.total) * 100;

  if (compact) {
    return (
      <div
        className={cn(
          'flex items-center gap-2 px-3 py-2 rounded-lg border transition-colors',
          node.status === 'online'
            ? 'border-status-online/30 bg-status-online/5'
            : 'border-status-offline/30 bg-status-offline/5'
        )}
      >
        <div
          className={cn(
            'w-2 h-2 rounded-full',
            node.status === 'online' ? 'bg-status-online' : 'bg-status-offline'
          )}
        />
        <span className="text-sm font-medium">{node.name}</span>
      </div>
    );
  }

  return (
    <div
      className={cn(
        'p-4 rounded-lg border transition-all',
        node.status === 'online'
          ? 'border-border bg-card hover:border-status-online/50'
          : 'border-status-offline/30 bg-status-offline/5'
      )}
    >
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <div
            className={cn(
              'w-10 h-10 rounded-lg flex items-center justify-center',
              node.status === 'online' ? 'bg-status-online/20' : 'bg-status-offline/20'
            )}
          >
            <div className="w-5 h-5 flex items-center justify-center">
              <div
                className={cn(
                  'w-3 h-3 rounded-full',
                  node.status === 'online' ? 'bg-status-online' : 'bg-status-offline'
                )}
              />
              <div
                className={cn(
                  'absolute w-5 h-5 border-2 rounded-full',
                  node.status === 'online' ? 'border-status-online/50' : 'border-status-offline/50'
                )}
              />
            </div>
          </div>
          <div>
            <h4 className="font-medium">{node.name}</h4>
            <p className="text-xs text-muted-foreground capitalize">{node.status}</p>
          </div>
        </div>
        <div
          className={cn(
            'px-2 py-1 rounded text-xs font-medium',
            node.status === 'online'
              ? 'bg-status-online/20 text-status-online'
              : 'bg-status-offline/20 text-status-offline'
          )}
        >
          {node.status === 'online' ? 'Online' : 'Offline'}
        </div>
      </div>

      {/* Stats */}
      <div className="flex flex-col gap-3">
        {/* Storage */}
        <div>
          <div className="flex justify-between text-sm mb-1">
            <span className="text-muted-foreground">Storage</span>
            <span>
              {node.storage.used.toFixed(1)} / {node.storage.total} GB
            </span>
          </div>
          <div className="h-2 bg-muted rounded-full overflow-hidden">
            <div
              className={cn(
                'h-full rounded-full transition-all',
                storagePercentage > 80
                  ? 'bg-status-warning'
                  : storagePercentage > 60
                  ? 'bg-status-warning/70'
                  : 'bg-primary'
              )}
              style={{ width: `${storagePercentage}%` }}
            />
          </div>
        </div>

        {/* Chunks */}
        <div className="flex justify-between text-sm">
          <span className="text-muted-foreground">Chunks stored</span>
          <span className="font-medium">{node.chunks}</span>
        </div>
      </div>
    </div>
  );
}
