'use client';

import { cn } from '@/lib/utils';
import type { FileInfo, NodeStatus } from '@/lib/types';

interface DistributionViewProps {
  files: FileInfo[];
  nodes: NodeStatus[];
}

export function DistributionView({ files, nodes }: DistributionViewProps) {
  if (files.length === 0) {
    return (
      <div className="text-center py-8 text-muted-foreground">
        No files to display distribution
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-4">
      {files.slice(0, 5).map((file) => (
        <FileDistributionRow key={file.id} file={file} nodes={nodes} />
      ))}
    </div>
  );
}

interface FileDistributionRowProps {
  file: FileInfo;
  nodes: NodeStatus[];
}

function FileDistributionRow({ file, nodes }: FileDistributionRowProps) {
  const getNodeName = (nodeId: string): string => {
    const node = nodes.find((n) => n.id === nodeId);
    return node?.name || `Node ${nodeId}`;
  };

  const getNodeStatus = (nodeId: string): 'online' | 'offline' => {
    const node = nodes.find((n) => n.id === nodeId);
    return node?.status || 'offline';
  };

  // Color map for different nodes
  const nodeColors: Record<string, string> = {
    '1': 'bg-chart-1',
    '2': 'bg-chart-2',
    '3': 'bg-chart-3',
  };

  return (
    <div className="p-4 rounded-lg border border-border bg-muted/20">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded bg-muted flex items-center justify-center">
            <div className="w-4 h-4 border-2 border-muted-foreground rounded-sm" />
          </div>
          <div>
            <p className="font-medium text-sm">{file.name}</p>
            <p className="text-xs text-muted-foreground">
              {formatSize(file.size)} - {file.chunks.length} chunks
            </p>
          </div>
        </div>
      </div>

      {/* Chunk Distribution Visualization */}
      <div className="flex flex-col gap-2">
        <div className="flex gap-1 h-8">
          {file.chunks.map((chunk, index) => {
            const nodeStatus = getNodeStatus(chunk.nodeId);
            return (
              <div
                key={chunk.id}
                className={cn(
                  'flex-1 rounded flex items-center justify-center text-xs font-mono transition-all',
                  nodeColors[chunk.nodeId] || 'bg-muted',
                  nodeStatus === 'offline' && 'opacity-50'
                )}
                title={`Chunk ${index + 1}: ${getNodeName(chunk.nodeId)} (${formatSize(chunk.size)})`}
              >
                <span className="text-primary-foreground text-[10px]">
                  C{index + 1}
                </span>
              </div>
            );
          })}
        </div>

        {/* Legend */}
        <div className="flex flex-wrap gap-3 mt-2">
          {file.chunks.map((chunk, index) => (
            <div key={chunk.id} className="flex items-center gap-2 text-xs">
              <div
                className={cn(
                  'w-3 h-3 rounded-sm',
                  nodeColors[chunk.nodeId] || 'bg-muted',
                  getNodeStatus(chunk.nodeId) === 'offline' && 'opacity-50'
                )}
              />
              <span className="text-muted-foreground">
                Chunk {index + 1}: {getNodeName(chunk.nodeId)}
                {getNodeStatus(chunk.nodeId) === 'offline' && ' (offline)'}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}
