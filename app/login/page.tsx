'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import Link from 'next/link';
import { LoginForm } from '@/components/auth/LoginForm';
import { authApi, getErrorMessage } from '@/services/api';
import storage from '@/lib/storage';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { useTheme } from '@/lib/theme-context';
import { auth } from '@/lib/firebase';
import { GoogleAuthProvider, signInWithPopup } from 'firebase/auth';

export default function LoginPage() {
  const router = useRouter();
  const [error, setError] = useState<string>();
  const { theme, toggleTheme } = useTheme();
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    // Redirect if already authenticated
    if (storage.isAuthenticated()) {
      router.push('/dashboard');
    }
  }, [router]);

  const handleLogin = async (email: string, password: string) => {
    setError(undefined);
    try {
      const response = await authApi.login(email, password);
      storage.setToken(response.token);
      storage.setUser(response.user);
      router.push('/dashboard');
    } catch (err) {
      setError(getErrorMessage(err, 'Invalid email or password. Please try again.'));
    }
  };

  const handleGoogleLogin = async () => {
    setError(undefined);
    try {
      const provider = new GoogleAuthProvider();
      const result = await signInWithPopup(auth, provider);
      const idToken = await result.user.getIdToken();

      const response = await authApi.loginWithFirebase(idToken);
      storage.setToken(response.token);
      storage.setUser(response.user);
      router.push('/dashboard');
    } catch (err: any) {
      if (err.code === 'auth/popup-closed-by-user') return;
      setError(getErrorMessage(err, 'Google login failed. Please try again.'));
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center p-4 bg-background">
      {/* Back to home link and theme toggle */}
      <div className="fixed top-4 left-4 right-4 flex items-center justify-between">
        <Link
          href="/"
          className="text-sm text-muted-foreground hover:text-foreground transition-colors flex items-center gap-2"
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
          </svg>
          Back to home
        </Link>

        {mounted && (
          <button
            onClick={toggleTheme}
            className="p-2 rounded-lg border border-border hover:bg-secondary transition-colors"
            aria-label="Toggle theme"
          >
            {theme === 'dark' ? (
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
              </svg>
            ) : (
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
              </svg>
            )}
          </button>
        )}
      </div>

      <div className="w-full max-w-md">
        {/* Logo / Brand */}
        <div className="flex flex-col items-center mb-8">
          <div className="flex items-center gap-2 mb-2">
            <div className="w-10 h-10 rounded-lg bg-primary flex items-center justify-center">
              <div className="w-5 h-5 border-2 border-primary-foreground rounded-sm" />
            </div>
            <span className="text-2xl font-semibold tracking-tight">NimbusFS</span>
          </div>
          <p className="text-muted-foreground text-sm">Distributed File Storage System</p>
        </div>

        <Card className="border-border bg-card">
          <CardHeader className="text-center">
            <CardTitle className="text-xl">Welcome back</CardTitle>
            <CardDescription>Sign in to access your files</CardDescription>
          </CardHeader>
          <CardContent>
            <LoginForm
              onSubmit={handleLogin}
              onGoogleLogin={handleGoogleLogin}
              error={error}
            />
          </CardContent>
        </Card>

        {/* Demo credentials */}
        <div className="mt-6 p-4 rounded-lg border border-border bg-card/50">
          <p className="text-xs text-muted-foreground mb-3 text-center font-medium">Working Demo Credentials</p>
          <div className="grid grid-cols-2 gap-4 text-xs">
            <div className="p-2 rounded bg-secondary/50">
              <p className="font-medium text-foreground mb-1">Admin Account</p>
              <p className="text-muted-foreground">admin@example.com</p>
              <p className="text-muted-foreground">admin</p>
            </div>
            <div className="p-2 rounded bg-secondary/50">
              <p className="font-medium text-foreground mb-1">User Account</p>
              <p className="text-muted-foreground">demo@example.com</p>
              <p className="text-muted-foreground">password</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
