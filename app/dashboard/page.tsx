'use client';

import { useState, useEffect, Suspense } from 'react';
import { useSearchParams } from 'next/navigation';
import { OverviewContent } from '@/components/dashboard/OverviewContent';
import { FilesContent } from '@/components/dashboard/FilesContent';
import { UploadContent } from '@/components/dashboard/UploadContent';
import { NodesContent } from '@/components/dashboard/NodesContent';
import { SettingsContent } from '@/components/dashboard/SettingsContent';
import { UsersContent } from '@/components/dashboard/UsersContent';
import { AdminFilesContent } from '@/components/dashboard/AdminFilesContent';
import storage from '@/lib/storage';
import type { DashboardTab, User } from '@/lib/types';

function DashboardContent() {
  const searchParams = useSearchParams();
  const [activeTab, setActiveTab] = useState<DashboardTab>('overview');
  const [user, setUser] = useState<User | null>(null);

  useEffect(() => {
    const tab = searchParams.get('tab') as DashboardTab;
    if (tab && ['overview', 'files', 'upload', 'nodes', 'settings', 'users', 'admin-files'].includes(tab)) {
      setActiveTab(tab);
    }
  }, [searchParams]);

  useEffect(() => {
    const storedUser = storage.getUser();
    if (storedUser) {
      setUser(storedUser);
    }
  }, []);

  const isAdmin = user?.role === 'admin';

  const renderContent = () => {
    switch (activeTab) {
      case 'overview':
        return <OverviewContent />;
      case 'files':
        return <FilesContent />;
      case 'upload':
        return <UploadContent />;
      case 'nodes':
        return <NodesContent />;
      case 'settings':
        return <SettingsContent />;
      case 'users':
        return isAdmin ? <UsersContent /> : <OverviewContent />;
      case 'admin-files':
        return isAdmin ? <AdminFilesContent /> : <OverviewContent />;
      default:
        return <OverviewContent />;
    }
  };

  return (
    <div className="flex-1 p-6 overflow-auto">
      {renderContent()}
    </div>
  );
}

export default function DashboardPage() {
  return (
    <Suspense fallback={
      <div className="flex items-center justify-center h-full">
        <div className="w-8 h-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
      </div>
    }>
      <DashboardContent />
    </Suspense>
  );
}
