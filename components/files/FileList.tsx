'use client';

import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Empty, EmptyDescription, EmptyMedia, EmptyTitle } from '@/components/ui/empty';
import type { FileInfo } from '@/lib/types';

interface FileListProps {
  files: FileInfo[];
  isLoading: boolean;
  onDownload: (file: FileInfo) => void;
  onDelete: (file: FileInfo) => void;
}

export function FileList({ files, isLoading, onDownload, onDelete }: FileListProps) {
  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="w-8 h-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }

  if (files.length === 0) {
    return (
      <Card className="border-border bg-card">
        <CardContent className="py-12">
          <Empty>
            <EmptyMedia>
              <div className="w-12 h-12 rounded-lg bg-muted flex items-center justify-center">
                <div className="w-6 h-6 border-2 border-muted-foreground rounded-sm" />
              </div>
            </EmptyMedia>
            <EmptyTitle>No files found</EmptyTitle>
            <EmptyDescription>
              Upload your first file to get started with distributed storage.
            </EmptyDescription>
          </Empty>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="border-border bg-card">
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="border-b border-border">
              <th className="text-left p-4 text-sm font-medium text-muted-foreground">Name</th>
              <th className="text-left p-4 text-sm font-medium text-muted-foreground">Size</th>
              <th className="text-left p-4 text-sm font-medium text-muted-foreground">Chunks</th>
              <th className="text-left p-4 text-sm font-medium text-muted-foreground">Uploaded</th>
              <th className="text-right p-4 text-sm font-medium text-muted-foreground">Actions</th>
            </tr>
          </thead>
          <tbody>
            {files.map((file) => (
              <FileRow
                key={file.id}
                file={file}
                onDownload={onDownload}
                onDelete={onDelete}
              />
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

interface FileRowProps {
  file: FileInfo;
  onDownload: (file: FileInfo) => void;
  onDelete: (file: FileInfo) => void;
}

function FileRow({ file, onDownload, onDelete }: FileRowProps) {
  const formatSize = (bytes: number): string => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
  };

  const formatDate = (dateString: string): string => {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  const getFileTypeIndicator = (fileName: string) => {
    const ext = fileName.split('.').pop()?.toLowerCase();
    const colors: Record<string, string> = {
      pdf: 'bg-red-500/20 text-red-400',
      csv: 'bg-green-500/20 text-green-400',
      json: 'bg-yellow-500/20 text-yellow-400',
      zip: 'bg-purple-500/20 text-purple-400',
      sql: 'bg-blue-500/20 text-blue-400',
    };
    return colors[ext || ''] || 'bg-muted text-muted-foreground';
  };

  return (
    <tr className="border-b border-border/50 hover:bg-muted/30 transition-colors">
      <td className="p-4">
        <div className="flex items-center gap-3">
          <div className={`w-8 h-8 rounded flex items-center justify-center text-xs font-mono ${getFileTypeIndicator(file.name)}`}>
            {file.name.split('.').pop()?.toUpperCase().slice(0, 3)}
          </div>
          <span className="font-medium">{file.name}</span>
        </div>
      </td>
      <td className="p-4 text-muted-foreground">{formatSize(file.size)}</td>
      <td className="p-4">
        <div className="flex items-center gap-1">
          {file.chunks.map((chunk, index) => (
            <div
              key={chunk.id}
              className="w-2 h-2 rounded-full bg-primary/60"
              title={`Chunk ${index + 1} on node ${chunk.nodeId}`}
            />
          ))}
          <span className="text-xs text-muted-foreground ml-2">
            {file.chunks.length} chunks
          </span>
        </div>
      </td>
      <td className="p-4 text-muted-foreground text-sm">{formatDate(file.uploadedAt)}</td>
      <td className="p-4">
        <div className="flex items-center justify-end gap-2">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => onDownload(file)}
            className="text-xs"
          >
            Download
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => onDelete(file)}
            className="text-xs text-destructive hover:text-destructive"
          >
            Delete
          </Button>
        </div>
      </td>
    </tr>
  );
}
