'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Field, FieldLabel } from '@/components/ui/field';
import { Input } from '@/components/ui/input';
import { Switch } from '@/components/ui/switch';
import storage from '@/lib/storage';
import type { User } from '@/lib/types';
import { fileApi, getErrorMessage, healthApi, profileApi } from '@/services/api';

export function SettingsContent() {
  const router = useRouter();
  const [apiUrl, setApiUrl] = useState('/api');
  const [notifications, setNotifications] = useState(true);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [user, setUser] = useState<User | null>(null);
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [isSaving, setIsSaving] = useState(false);
  const [saveMessage, setSaveMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);

  useEffect(() => {
    const loadProfile = async () => {
      try {
        const profile = await profileApi.getProfile();
        setUser(profile.user);
        setName(profile.user.name || '');
        setEmail(profile.user.email);
        setApiUrl(profile.apiUrl);
      } catch {
        const storedUser = storage.getUser();
        if (storedUser) {
          setUser(storedUser);
          setName(storedUser.name || '');
          setEmail(storedUser.email);
        }
      }
    };

    void loadProfile();
  }, []);

  const showMessage = (type: 'success' | 'error', text: string) => {
    setSaveMessage({ type, text });
    window.setTimeout(() => setSaveMessage(null), 3000);
  };

  const handleSave = async () => {
    setIsSaving(true);
    setSaveMessage(null);

    try {
      const updatedUser = await profileApi.updateProfile({ email, name });
      storage.setUser(updatedUser);
      setUser(updatedUser);
      showMessage('success', 'Settings saved successfully.');
    } catch (err) {
      showMessage('error', getErrorMessage(err, 'Unable to save your profile.'));
    } finally {
      setIsSaving(false);
    }
  };

  const handleTestConnection = async () => {
    try {
      await healthApi.getHealth();
      showMessage('success', 'Backend connection is healthy.');
    } catch (err) {
      showMessage('error', getErrorMessage(err, 'Unable to reach the backend.'));
    }
  };

  const handleDeleteFiles = async () => {
    if (!confirm('Delete all of your files? This action cannot be undone.')) {
      return;
    }

    try {
      await fileApi.deleteAllFiles();
      showMessage('success', 'All of your files have been deleted.');
    } catch (err) {
      showMessage('error', getErrorMessage(err, 'Unable to delete your files.'));
    }
  };

  const handleDeleteAccount = async () => {
    if (!confirm('Delete your account and all associated data? This action cannot be undone.')) {
      return;
    }

    try {
      await profileApi.deleteAccount();
      storage.clearAuth();
      router.push('/login');
    } catch (err) {
      showMessage('error', getErrorMessage(err, 'Unable to delete your account.'));
    }
  };

  return (
    <div className="flex flex-col gap-6 max-w-3xl">
      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle className="text-base">Profile</CardTitle>
          <CardDescription>Your personal information and account details</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-6">
          <div className="flex items-start gap-6">
            <div className="w-20 h-20 rounded-full bg-primary/20 border-2 border-primary/30 flex items-center justify-center">
              <span className="text-2xl font-semibold text-primary">
                {(name || email)?.charAt(0).toUpperCase()}
              </span>
            </div>
            <div className="flex-1">
              <h3 className="text-lg font-medium">{name || email}</h3>
              <p className="text-sm text-muted-foreground">{email}</p>
              <div className="mt-2 flex items-center gap-2">
                <span
                  className={`inline-flex px-2 py-1 text-xs rounded-full ${
                    user?.role === 'admin'
                      ? 'bg-primary/20 text-primary'
                      : 'bg-secondary text-muted-foreground'
                  }`}
                >
                  {user?.role || 'user'}
                </span>
                {user?.role === 'admin' && (
                  <span className="text-xs text-muted-foreground">Full system access</span>
                )}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle className="text-base">Account Settings</CardTitle>
          <CardDescription>Update your account information</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <Field>
            <FieldLabel>Display Name</FieldLabel>
            <Input
              type="text"
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder="Enter your name"
              className="bg-secondary border-border"
            />
          </Field>

          <Field>
            <FieldLabel>Email Address</FieldLabel>
            <Input
              type="email"
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              className="bg-secondary border-border"
            />
          </Field>

          <Field>
            <FieldLabel>Password</FieldLabel>
            <Input
              type="password"
              value="********"
              disabled
              className="bg-secondary border-border"
            />
            <p className="mt-2 text-xs text-muted-foreground">
              Password management is not exposed in this local demo yet.
            </p>
          </Field>
        </CardContent>
      </Card>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle className="text-base">API Configuration</CardTitle>
          <CardDescription>Configure the backend API endpoint</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <Field>
            <FieldLabel>API Base URL</FieldLabel>
            <Input
              type="url"
              value={apiUrl}
              onChange={(event) => setApiUrl(event.target.value)}
              placeholder="/api"
              className="bg-secondary border-border font-mono text-sm"
            />
          </Field>
          <div className="flex items-center gap-3">
            <Button variant="outline" size="sm" onClick={() => void handleTestConnection()}>
              Test Connection
            </Button>
            <span className="text-xs text-muted-foreground">
              Verify connectivity to your NimbusFS backend
            </span>
          </div>
        </CardContent>
      </Card>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle className="text-base">Preferences</CardTitle>
          <CardDescription>Customize your dashboard experience</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium">Enable Notifications</p>
              <p className="text-xs text-muted-foreground">
                Receive alerts for upload completion and node status changes
              </p>
            </div>
            <Switch checked={notifications} onCheckedChange={setNotifications} />
          </div>

          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium">Auto-refresh Data</p>
              <p className="text-xs text-muted-foreground">
                Automatically refresh node status every 30 seconds
              </p>
            </div>
            <Switch checked={autoRefresh} onCheckedChange={setAutoRefresh} />
          </div>
        </CardContent>
      </Card>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle className="text-base">Danger Zone</CardTitle>
          <CardDescription>Irreversible actions that affect your account</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium">Delete All Files</p>
              <p className="text-xs text-muted-foreground">
                Permanently remove all your files from the system
              </p>
            </div>
            <Button
              variant="outline"
              size="sm"
              className="text-destructive border-destructive/30 hover:bg-destructive/10"
              onClick={() => void handleDeleteFiles()}
            >
              Delete Files
            </Button>
          </div>
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium">Delete Account</p>
              <p className="text-xs text-muted-foreground">
                Permanently delete your account and all associated data
              </p>
            </div>
            <Button variant="destructive" size="sm" onClick={() => void handleDeleteAccount()}>
              Delete Account
            </Button>
          </div>
        </CardContent>
      </Card>

      {saveMessage && (
        <div
          className={`p-3 rounded-lg text-sm ${
            saveMessage.type === 'success'
              ? 'bg-status-online/20 text-status-online border border-status-online/30'
              : 'bg-destructive/20 text-destructive border border-destructive/30'
          }`}
        >
          {saveMessage.text}
        </div>
      )}

      <div className="flex gap-3">
        <Button onClick={() => void handleSave()} disabled={isSaving}>
          {isSaving ? 'Saving...' : 'Save Changes'}
        </Button>
        <Button
          variant="outline"
          onClick={() => {
            setName(user?.name || '');
            setEmail(user?.email || '');
            setSaveMessage(null);
          }}
        >
          Cancel
        </Button>
      </div>
    </div>
  );
}
