'use client';

import { useEffect, useState } from 'react';

import { DistributionView } from '@/components/distribution/DistributionView';
import { NodeStatusPanel } from '@/components/nodes/NodeStatusPanel';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import type { FileInfo, NodeStatus } from '@/lib/types';
import { fileApi, getErrorMessage, healthApi } from '@/services/api';

export function OverviewContent() {
  const [nodes, setNodes] = useState<NodeStatus[]>([]);
  const [files, setFiles] = useState<FileInfo[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string>();

  useEffect(() => {
    const loadData = async () => {
      setIsLoading(true);
      setError(undefined);

      try {
        const [health, fileList] = await Promise.all([
          healthApi.getHealth(),
          fileApi.getFiles(),
        ]);

        setNodes(health.nodes);
        setFiles(fileList);
      } catch (err) {
        setError(getErrorMessage(err, 'Unable to load dashboard overview.'));
      } finally {
        setIsLoading(false);
      }
    };

    void loadData();
  }, []);

  const totalStorage = nodes.reduce((acc, node) => acc + node.storage.total, 0);
  const usedStorage = nodes.reduce((acc, node) => acc + node.storage.used, 0);
  const onlineNodes = nodes.filter((node) => node.status === 'online').length;
  const totalChunks = nodes.reduce((acc, node) => acc + node.chunks, 0);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="w-8 h-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }

  if (error) {
    return (
      <Card className="border-destructive/30 bg-destructive/5">
        <CardContent className="pt-6 text-sm text-destructive">{error}</CardContent>
      </Card>
    );
  }

  return (
    <div className="flex flex-col gap-6">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          title="Total Files"
          value={files.length.toString()}
          subtitle={files.length > 0 ? 'Available in your workspace' : 'Upload your first file'}
        />
        <StatCard
          title="Storage Used"
          value={`${usedStorage.toFixed(2)} GB`}
          subtitle={`of ${totalStorage.toFixed(0)} GB total`}
        />
        <StatCard
          title="Active Nodes"
          value={`${onlineNodes}/${nodes.length}`}
          subtitle={onlineNodes === nodes.length ? 'All systems operational' : 'Some nodes offline'}
          status={onlineNodes === nodes.length ? 'online' : 'warning'}
        />
        <StatCard
          title="Total Chunks"
          value={totalChunks.toString()}
          subtitle="Distributed across storage nodes"
        />
      </div>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle className="text-base">Node Status</CardTitle>
        </CardHeader>
        <CardContent>
          <NodeStatusPanel nodes={nodes} />
        </CardContent>
      </Card>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle className="text-base">Recent File Distribution</CardTitle>
        </CardHeader>
        <CardContent>
          <DistributionView files={files} nodes={nodes} />
        </CardContent>
      </Card>
    </div>
  );
}

interface StatCardProps {
  title: string;
  value: string;
  subtitle: string;
  status?: 'online' | 'offline' | 'warning';
}

function StatCard({ title, value, subtitle, status }: StatCardProps) {
  return (
    <Card className="border-border bg-card">
      <CardContent className="pt-6">
        <p className="text-sm text-muted-foreground">{title}</p>
        <div className="flex items-baseline gap-2 mt-1">
          <p className="text-2xl font-semibold">{value}</p>
          {status && (
            <div
              className={`w-2 h-2 rounded-full ${
                status === 'online'
                  ? 'bg-status-online'
                  : status === 'warning'
                    ? 'bg-status-warning'
                    : 'bg-status-offline'
              }`}
            />
          )}
        </div>
        <p className="text-xs text-muted-foreground mt-1">{subtitle}</p>
      </CardContent>
    </Card>
  );
}
