'use client';

import { useState } from 'react';

import { FileUpload } from '@/components/files/FileUpload';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { fileApi, getErrorMessage } from '@/services/api';

interface UploadedFile {
  error?: string;
  id: string;
  name: string;
  progress: number;
  size: number;
  status: 'uploading' | 'complete' | 'error';
}

export function UploadContent() {
  const [uploads, setUploads] = useState<UploadedFile[]>([]);
  const [message, setMessage] = useState<string>();

  const handleFileSelect = async (files: File[]) => {
    setMessage(undefined);

    for (const file of files) {
      const uploadId = crypto.randomUUID();

      setUploads((prev) => [
        {
          id: uploadId,
          name: file.name,
          size: file.size,
          status: 'uploading',
          progress: 0,
        },
        ...prev,
      ]);

      try {
        await fileApi.uploadFile(file, (progress) => {
          setUploads((prev) =>
            prev.map((upload) =>
              upload.id === uploadId
                ? { ...upload, progress: progress.percentage, status: 'uploading' }
                : upload,
            ),
          );
        });

        setUploads((prev) =>
          prev.map((upload) =>
            upload.id === uploadId
              ? { ...upload, progress: 100, status: 'complete' }
              : upload,
          ),
        );
        setMessage('Upload complete. Your files are now available in My Files.');
      } catch (err) {
        const errorMessage = getErrorMessage(err, `Unable to upload ${file.name}.`);
        setUploads((prev) =>
          prev.map((upload) =>
            upload.id === uploadId
              ? { ...upload, status: 'error', error: errorMessage }
              : upload,
          ),
        );
      }
    }
  };

  const clearCompleted = () => {
    setUploads((prev) => prev.filter((upload) => upload.status !== 'complete'));
  };

  return (
    <div className="flex flex-col gap-6 max-w-3xl">
      <FileUpload onFileSelect={handleFileSelect} />

      {message && (
        <div className="rounded-md border border-status-online/30 bg-status-online/10 p-3 text-sm text-status-online">
          {message}
        </div>
      )}

      {uploads.length > 0 && (
        <Card className="border-border bg-card">
          <CardHeader className="flex flex-row items-center justify-between">
            <div>
              <CardTitle className="text-base">Upload Queue</CardTitle>
              <CardDescription>
                {uploads.filter((upload) => upload.status === 'uploading').length} uploading,{' '}
                {uploads.filter((upload) => upload.status === 'complete').length} complete
              </CardDescription>
            </div>
            {uploads.some((upload) => upload.status === 'complete') && (
              <button
                onClick={clearCompleted}
                className="text-sm text-muted-foreground hover:text-foreground transition-colors"
              >
                Clear completed
              </button>
            )}
          </CardHeader>
          <CardContent>
            <div className="flex flex-col gap-3">
              {uploads.map((upload) => (
                <UploadItem key={upload.id} upload={upload} />
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle className="text-base">How it works</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="flex flex-col gap-2">
              <div className="w-8 h-8 rounded-lg bg-primary/20 flex items-center justify-center text-primary font-semibold">
                1
              </div>
              <h4 className="font-medium">Upload</h4>
              <p className="text-sm text-muted-foreground">
                Drag and drop files or click to browse. Uploads are persisted by the local API.
              </p>
            </div>
            <div className="flex flex-col gap-2">
              <div className="w-8 h-8 rounded-lg bg-primary/20 flex items-center justify-center text-primary font-semibold">
                2
              </div>
              <h4 className="font-medium">Chunk</h4>
              <p className="text-sm text-muted-foreground">
                Each file is split into chunks and assigned to nodes for distribution metadata.
              </p>
            </div>
            <div className="flex flex-col gap-2">
              <div className="w-8 h-8 rounded-lg bg-primary/20 flex items-center justify-center text-primary font-semibold">
                3
              </div>
              <h4 className="font-medium">Store</h4>
              <p className="text-sm text-muted-foreground">
                The file is stored locally and becomes immediately available for listing and download.
              </p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

interface UploadItemProps {
  upload: UploadedFile;
}

function UploadItem({ upload }: UploadItemProps) {
  const formatSize = (bytes: number): string => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  return (
    <div className="flex flex-col gap-2 p-3 rounded-lg bg-muted/30 border border-border/50">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded bg-muted flex items-center justify-center">
            <div className="w-4 h-4 border-2 border-muted-foreground rounded-sm" />
          </div>
          <div>
            <p className="font-medium text-sm">{upload.name}</p>
            <p className="text-xs text-muted-foreground">{formatSize(upload.size)}</p>
            {upload.error && <p className="text-xs text-destructive mt-1">{upload.error}</p>}
          </div>
        </div>
        <div className="flex items-center gap-2">
          {upload.status === 'complete' ? (
            <span className="text-xs text-status-online font-medium">Complete</span>
          ) : upload.status === 'error' ? (
            <span className="text-xs text-destructive font-medium">Error</span>
          ) : (
            <span className="text-xs text-muted-foreground">{upload.progress}%</span>
          )}
        </div>
      </div>

      <div className="h-1.5 bg-muted rounded-full overflow-hidden">
        <div
          className={`h-full transition-all duration-300 ${
            upload.status === 'complete'
              ? 'bg-status-online'
              : upload.status === 'error'
                ? 'bg-destructive'
                : 'bg-primary'
          }`}
          style={{ width: `${upload.progress}%` }}
        />
      </div>
    </div>
  );
}
