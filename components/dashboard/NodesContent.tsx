'use client';

import { useEffect, useState } from 'react';

import { NodeStatusPanel } from '@/components/nodes/NodeStatusPanel';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import type { NodeStatus } from '@/lib/types';
import { getErrorMessage, healthApi } from '@/services/api';

export function NodesContent() {
  const [nodes, setNodes] = useState<NodeStatus[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date());
  const [error, setError] = useState<string>();

  const loadNodes = async () => {
    setIsLoading(true);
    setError(undefined);

    try {
      const result = await healthApi.getNodes();
      setNodes(result);
      setLastRefresh(new Date());
    } catch (err) {
      setError(getErrorMessage(err, 'Unable to load node status.'));
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    void loadNodes();
  }, []);

  const totalStorage = nodes.reduce((acc, node) => acc + node.storage.total, 0);
  const usedStorage = nodes.reduce((acc, node) => acc + node.storage.used, 0);
  const totalChunks = nodes.reduce((acc, node) => acc + node.chunks, 0);
  const onlineNodes = nodes.filter((node) => node.status === 'online').length;

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="w-8 h-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-6">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-muted-foreground">
            Last refreshed: {lastRefresh.toLocaleTimeString()}
          </p>
        </div>
        <Button variant="outline" onClick={() => void loadNodes()}>
          Refresh Status
        </Button>
      </div>

      {error && (
        <div className="rounded-md bg-destructive/10 border border-destructive/20 p-3">
          <p className="text-sm text-destructive">{error}</p>
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <StatCard
          title="Total Nodes"
          value={nodes.length.toString()}
          subtitle={`${onlineNodes} online, ${nodes.length - onlineNodes} offline`}
        />
        <StatCard
          title="Storage Capacity"
          value={`${totalStorage.toFixed(0)} GB`}
          subtitle={`${usedStorage.toFixed(2)} GB used (${totalStorage ? ((usedStorage / totalStorage) * 100).toFixed(0) : 0}%)`}
        />
        <StatCard
          title="Total Chunks"
          value={totalChunks.toString()}
          subtitle="Distributed across nodes"
        />
        <StatCard
          title="System Health"
          value={onlineNodes === nodes.length ? 'Healthy' : 'Degraded'}
          subtitle={onlineNodes === nodes.length ? 'All nodes operational' : 'Some nodes offline'}
          status={onlineNodes === nodes.length ? 'online' : 'warning'}
        />
      </div>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle className="text-base">Node Details</CardTitle>
          <CardDescription>Detailed status and storage information for each node</CardDescription>
        </CardHeader>
        <CardContent>
          <NodeStatusPanel nodes={nodes} />
        </CardContent>
      </Card>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle className="text-base">Storage Distribution</CardTitle>
          <CardDescription>Visual representation of storage usage across nodes</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex flex-col gap-4">
            <div className="flex items-end gap-4 h-40">
              {nodes.map((node) => {
                const heightPercentage = node.storage.total
                  ? (node.storage.used / node.storage.total) * 100
                  : 0;

                return (
                  <div key={node.id} className="flex-1 flex flex-col items-center gap-2">
                    <div className="relative w-full h-32 bg-muted rounded-t-lg overflow-hidden">
                      <div
                        className={`absolute bottom-0 left-0 right-0 transition-all duration-500 ${
                          node.status === 'online' ? 'bg-primary' : 'bg-muted-foreground'
                        }`}
                        style={{ height: `${heightPercentage}%` }}
                      />
                    </div>
                    <div className="text-center">
                      <p className="text-sm font-medium">{node.name}</p>
                      <p className="text-xs text-muted-foreground">
                        {node.storage.used.toFixed(2)} / {node.storage.total.toFixed(0)} GB
                      </p>
                    </div>
                  </div>
                );
              })}
            </div>

            <div className="flex items-center justify-center gap-6 pt-4 border-t border-border">
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 rounded bg-primary" />
                <span className="text-xs text-muted-foreground">Used Storage</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 rounded bg-muted" />
                <span className="text-xs text-muted-foreground">Available Storage</span>
              </div>
            </div>
          </div>
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
