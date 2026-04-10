'use client';

import { cn } from '@/lib/utils';
import type { DashboardTab, UserRole } from '@/lib/types';

interface NavItem {
  id: DashboardTab;
  label: string;
  adminOnly?: boolean;
}

const navItems: NavItem[] = [
  { id: 'overview', label: 'Overview' },
  { id: 'files', label: 'My Files' },
  { id: 'upload', label: 'Upload' },
  { id: 'nodes', label: 'Nodes' },
  { id: 'settings', label: 'Settings' },
];

const adminNavItems: NavItem[] = [
  { id: 'users', label: 'Manage Users', adminOnly: true },
  { id: 'admin-files', label: 'All Files', adminOnly: true },
];

interface SidebarProps {
  activeTab: DashboardTab;
  onTabChange: (tab: DashboardTab) => void;
  userRole?: UserRole;
}

export function Sidebar({ activeTab, onTabChange, userRole }: SidebarProps) {
  const isAdmin = userRole === 'admin';

  return (
    <aside className="w-64 h-screen bg-sidebar border-r border-sidebar-border flex flex-col">
      {/* Logo */}
      <div className="p-6 border-b border-sidebar-border">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 rounded-lg bg-primary flex items-center justify-center">
            <div className="w-4 h-4 border-2 border-primary-foreground rounded-sm" />
          </div>
          <span className="font-semibold text-lg">NimbusFS</span>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 p-4 overflow-y-auto">
        <ul className="flex flex-col gap-1">
          {navItems.map((item) => (
            <li key={item.id}>
              <button
                onClick={() => onTabChange(item.id)}
                className={cn(
                  'w-full flex items-center gap-3 px-3 py-2.5 rounded-md text-sm font-medium transition-colors',
                  activeTab === item.id
                    ? 'bg-sidebar-accent text-sidebar-accent-foreground'
                    : 'text-sidebar-foreground/70 hover:text-sidebar-foreground hover:bg-sidebar-accent/50'
                )}
              >
                <NavIcon id={item.id} isActive={activeTab === item.id} />
                {item.label}
              </button>
            </li>
          ))}
        </ul>

        {/* Admin Section */}
        {isAdmin && (
          <>
            <div className="my-4 px-3">
              <div className="flex items-center gap-2">
                <div className="h-px flex-1 bg-sidebar-border" />
                <span className="text-xs text-sidebar-foreground/50 uppercase tracking-wider">Admin</span>
                <div className="h-px flex-1 bg-sidebar-border" />
              </div>
            </div>
            <ul className="flex flex-col gap-1">
              {adminNavItems.map((item) => (
                <li key={item.id}>
                  <button
                    onClick={() => onTabChange(item.id)}
                    className={cn(
                      'w-full flex items-center gap-3 px-3 py-2.5 rounded-md text-sm font-medium transition-colors',
                      activeTab === item.id
                        ? 'bg-primary/20 text-primary'
                        : 'text-sidebar-foreground/70 hover:text-sidebar-foreground hover:bg-sidebar-accent/50'
                    )}
                  >
                    <NavIcon id={item.id} isActive={activeTab === item.id} />
                    {item.label}
                  </button>
                </li>
              ))}
            </ul>
          </>
        )}
      </nav>

      {/* Footer with branding */}
      <div className="p-4 border-t border-sidebar-border">
        <p className="text-xs text-sidebar-foreground/50 text-center">
          NimbusFS v1.0
        </p>
      </div>
    </aside>
  );
}

function NavIcon({ id, isActive }: { id: DashboardTab; isActive: boolean }) {
  const baseClass = cn(
    'w-5 h-5 flex items-center justify-center',
    isActive ? 'text-primary' : 'text-current'
  );

  switch (id) {
    case 'overview':
      return (
        <div className={baseClass}>
          <div className="w-4 h-4 grid grid-cols-2 gap-0.5">
            <div className="bg-current rounded-sm" />
            <div className="bg-current rounded-sm" />
            <div className="bg-current rounded-sm" />
            <div className="bg-current rounded-sm" />
          </div>
        </div>
      );
    case 'files':
      return (
        <div className={baseClass}>
          <div className="w-4 h-4 border-2 border-current rounded-sm relative">
            <div className="absolute top-0 right-0 w-1.5 h-1.5 bg-sidebar border-l border-b border-current" />
          </div>
        </div>
      );
    case 'upload':
      return (
        <div className={baseClass}>
          <div className="w-4 h-4 flex flex-col items-center justify-center">
            <div className="w-0 h-0 border-l-[5px] border-l-transparent border-r-[5px] border-r-transparent border-b-[6px] border-b-current" />
            <div className="w-0.5 h-2 bg-current mt-[-2px]" />
          </div>
        </div>
      );
    case 'nodes':
      return (
        <div className={baseClass}>
          <div className="w-4 h-4 flex items-center justify-center relative">
            <div className="w-2 h-2 bg-current rounded-full" />
            <div className="absolute w-4 h-4 border-2 border-current rounded-full" />
          </div>
        </div>
      );
    case 'settings':
      return (
        <div className={baseClass}>
          <div className="w-4 h-4 border-2 border-current rounded-full flex items-center justify-center">
            <div className="w-1.5 h-1.5 bg-current rounded-full" />
          </div>
        </div>
      );
    case 'users':
      return (
        <div className={baseClass}>
          <div className="w-4 h-4 flex items-end justify-center gap-0.5">
            <div className="w-1.5 h-2.5 bg-current rounded-t-full" />
            <div className="w-1.5 h-3 bg-current rounded-t-full" />
            <div className="w-1.5 h-2 bg-current rounded-t-full" />
          </div>
        </div>
      );
    case 'admin-files':
      return (
        <div className={baseClass}>
          <div className="w-4 h-4 flex flex-col gap-0.5">
            <div className="h-1 bg-current rounded-sm w-full" />
            <div className="h-1 bg-current rounded-sm w-3" />
            <div className="h-1 bg-current rounded-sm w-full" />
          </div>
        </div>
      );
    default:
      return <div className={baseClass} />;
  }
}
