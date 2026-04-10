'use client';

import { useEffect, useState } from 'react';

import { FileList } from '@/components/files/FileList';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import type { FileInfo } from '@/lib/types';
import { fileApi, getErrorMessage } from '@/services/api';

export function FilesContent() {
  const [files, setFiles] = useState<FileInfo[]>([]);
  const [filteredFiles, setFilteredFiles] = useState<FileInfo[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string>();

  useEffect(() => {
    const loadFiles = async () => {
      setIsLoading(true);
      setError(undefined);

      try {
        const result = await fileApi.getFiles();
        setFiles(result);
        setFilteredFiles(result);
      } catch (err) {
        setError(getErrorMessage(err, 'Unable to load files.'));
      } finally {
        setIsLoading(false);
      }
    };

    void loadFiles();
  }, []);

  useEffect(() => {
    if (!searchQuery) {
      setFilteredFiles(files);
      return;
    }

    const lowered = searchQuery.toLowerCase();
    setFilteredFiles(files.filter((file) => file.name.toLowerCase().includes(lowered)));
  }, [files, searchQuery]);

  const handleDownload = async (file: FileInfo) => {
    try {
      await fileApi.downloadFile(file.id, file.name);
    } catch (err) {
      setError(getErrorMessage(err, 'Unable to download the selected file.'));
    }
  };

  const handleDelete = async (file: FileInfo) => {
    if (!confirm(`Are you sure you want to delete ${file.name}?`)) {
      return;
    }

    try {
      await fileApi.deleteFile(file.id);
      setFiles((prev) => prev.filter((candidate) => candidate.id !== file.id));
      setError(undefined);
    } catch (err) {
      setError(getErrorMessage(err, 'Unable to delete the selected file.'));
    }
  };

  return (
    <div className="flex flex-col gap-6">
      <div className="flex items-center gap-4">
        <div className="flex-1 max-w-md">
          <Input
            type="search"
            placeholder="Search files..."
            value={searchQuery}
            onChange={(event) => setSearchQuery(event.target.value)}
            className="bg-secondary border-border"
          />
        </div>
        <Button variant="outline" onClick={() => setSearchQuery('')}>
          Clear
        </Button>
      </div>

      {error && (
        <div className="rounded-md bg-destructive/10 border border-destructive/20 p-3">
          <p className="text-sm text-destructive">{error}</p>
        </div>
      )}

      <FileList
        files={filteredFiles}
        isLoading={isLoading}
        onDownload={handleDownload}
        onDelete={handleDelete}
      />
    </div>
  );
}
