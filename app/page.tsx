'use client';

import Link from 'next/link';
import { useEffect, useState } from 'react';
import { useTheme } from '@/lib/theme-context';

export default function LandingPage() {
  const { theme, toggleTheme } = useTheme();
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  return (
    <div className="min-h-screen bg-background text-foreground">
      {/* Navigation */}
      <nav className="fixed top-0 left-0 right-0 z-50 border-b border-border bg-background/80 backdrop-blur-sm">
        <div className="max-w-7xl mx-auto px-6 h-16 flex items-center justify-between">
          <Link href="/" className="flex items-center gap-2">
            <div className="w-8 h-8 rounded-lg bg-primary flex items-center justify-center">
              <div className="w-4 h-4 border-2 border-primary-foreground rounded-sm" />
            </div>
            <span className="font-semibold text-lg">NimbusFS</span>
          </Link>
          
          <div className="hidden md:flex items-center gap-8">
            <a href="#features" className="text-sm text-muted-foreground hover:text-foreground transition-colors">Features</a>
            <a href="#how-it-works" className="text-sm text-muted-foreground hover:text-foreground transition-colors">How It Works</a>
            <a href="#pricing" className="text-sm text-muted-foreground hover:text-foreground transition-colors">Pricing</a>
          </div>
          
          <div className="flex items-center gap-4">
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
            <Link href="/login" className="text-sm font-medium hover:text-primary transition-colors">
              Log in
            </Link>
            <Link href="/login" className="px-4 py-2 rounded-lg bg-primary text-primary-foreground text-sm font-medium hover:opacity-90 transition-opacity">
              Get Started
            </Link>
          </div>
        </div>
      </nav>

      {/* Hero Section */}
      <section className="pt-32 pb-20 px-6">
        <div className="max-w-5xl mx-auto text-center">
          <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-primary/10 border border-primary/20 mb-8">
            <span className="text-sm text-primary font-medium">New: Multi-region replication now available</span>
          </div>
          
          <h1 className="text-4xl md:text-6xl lg:text-7xl font-bold tracking-tight text-balance mb-6">
            Distributed file storage
            <br />
            <span className="text-primary">built for scale</span>
          </h1>
          
          <p className="text-lg md:text-xl text-muted-foreground max-w-2xl mx-auto mb-10 text-pretty">
            Store, distribute, and manage your files across multiple nodes with automatic chunk distribution, redundancy, and blazing fast retrieval.
          </p>
          
          <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
            <Link href="/login" className="w-full sm:w-auto px-8 py-3 rounded-lg bg-primary text-primary-foreground font-medium hover:opacity-90 transition-opacity">
              Start for free
            </Link>
            <a href="#how-it-works" className="w-full sm:w-auto px-8 py-3 rounded-lg border border-border font-medium hover:bg-secondary transition-colors">
              See how it works
            </a>
          </div>
        </div>
      </section>

      {/* Stats Section */}
      <section className="py-16 border-y border-border bg-card">
        <div className="max-w-7xl mx-auto px-6">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-8">
            <div className="text-center">
              <p className="text-3xl md:text-4xl font-bold text-primary">99.99%</p>
              <p className="text-sm text-muted-foreground mt-1">Uptime SLA</p>
            </div>
            <div className="text-center">
              <p className="text-3xl md:text-4xl font-bold text-primary">3x</p>
              <p className="text-sm text-muted-foreground mt-1">Data redundancy</p>
            </div>
            <div className="text-center">
              <p className="text-3xl md:text-4xl font-bold text-primary">50ms</p>
              <p className="text-sm text-muted-foreground mt-1">Average latency</p>
            </div>
            <div className="text-center">
              <p className="text-3xl md:text-4xl font-bold text-primary">10TB+</p>
              <p className="text-sm text-muted-foreground mt-1">Files stored</p>
            </div>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <section id="features" className="py-24 px-6">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-4xl font-bold mb-4">Everything you need for distributed storage</h2>
            <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
              Built from the ground up for reliability, performance, and ease of use.
            </p>
          </div>
          
          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6">
            <div className="p-6 rounded-xl border border-border bg-card hover:border-primary/50 transition-colors">
              <div className="w-12 h-12 rounded-lg bg-primary/10 flex items-center justify-center mb-4">
                <div className="w-6 h-6 grid grid-cols-2 gap-1">
                  <div className="bg-primary rounded-sm" />
                  <div className="bg-primary rounded-sm" />
                  <div className="bg-primary rounded-sm" />
                  <div className="bg-primary rounded-sm" />
                </div>
              </div>
              <h3 className="text-lg font-semibold mb-2">Automatic Chunking</h3>
              <p className="text-muted-foreground text-sm">
                Files are automatically split into optimally-sized chunks and distributed across multiple storage nodes.
              </p>
            </div>

            <div className="p-6 rounded-xl border border-border bg-card hover:border-primary/50 transition-colors">
              <div className="w-12 h-12 rounded-lg bg-primary/10 flex items-center justify-center mb-4">
                <div className="w-6 h-6 flex items-center justify-center relative">
                  <div className="w-4 h-4 border-2 border-primary rounded-full" />
                  <div className="absolute w-2 h-2 bg-primary rounded-full" />
                </div>
              </div>
              <h3 className="text-lg font-semibold mb-2">Multi-Node Storage</h3>
              <p className="text-muted-foreground text-sm">
                Your data is stored across multiple nodes for redundancy. If one node fails, your files remain accessible.
              </p>
            </div>

            <div className="p-6 rounded-xl border border-border bg-card hover:border-primary/50 transition-colors">
              <div className="w-12 h-12 rounded-lg bg-primary/10 flex items-center justify-center mb-4">
                <div className="w-6 h-6 border-2 border-primary rounded flex items-center justify-center">
                  <div className="w-2 h-3 border-r-2 border-b-2 border-primary transform rotate-45 -translate-y-0.5" />
                </div>
              </div>
              <h3 className="text-lg font-semibold mb-2">Real-time Monitoring</h3>
              <p className="text-muted-foreground text-sm">
                Monitor node health, storage usage, and file distribution in real-time through our intuitive dashboard.
              </p>
            </div>

            <div className="p-6 rounded-xl border border-border bg-card hover:border-primary/50 transition-colors">
              <div className="w-12 h-12 rounded-lg bg-primary/10 flex items-center justify-center mb-4">
                <div className="w-6 h-6 flex flex-col items-center justify-center">
                  <div className="w-0 h-0 border-l-[8px] border-l-transparent border-r-[8px] border-r-transparent border-b-[10px] border-b-primary" />
                  <div className="w-1 h-2 bg-primary -mt-1" />
                </div>
              </div>
              <h3 className="text-lg font-semibold mb-2">Easy Uploads</h3>
              <p className="text-muted-foreground text-sm">
                Drag and drop files or use our API. Large file uploads are handled seamlessly with progress tracking.
              </p>
            </div>

            <div className="p-6 rounded-xl border border-border bg-card hover:border-primary/50 transition-colors">
              <div className="w-12 h-12 rounded-lg bg-primary/10 flex items-center justify-center mb-4">
                <div className="w-6 h-6 border-2 border-primary rounded-lg flex items-center justify-center">
                  <div className="w-3 h-0.5 bg-primary" />
                </div>
              </div>
              <h3 className="text-lg font-semibold mb-2">Admin Controls</h3>
              <p className="text-muted-foreground text-sm">
                Manage users, set permissions, and control access to files with our comprehensive admin dashboard.
              </p>
            </div>

            <div className="p-6 rounded-xl border border-border bg-card hover:border-primary/50 transition-colors">
              <div className="w-12 h-12 rounded-lg bg-primary/10 flex items-center justify-center mb-4">
                <div className="w-6 h-6 flex flex-col gap-1">
                  <div className="h-1.5 bg-primary rounded-full w-full" />
                  <div className="h-1.5 bg-primary rounded-full w-4" />
                  <div className="h-1.5 bg-primary rounded-full w-5" />
                </div>
              </div>
              <h3 className="text-lg font-semibold mb-2">Detailed Analytics</h3>
              <p className="text-muted-foreground text-sm">
                Track storage usage, upload/download patterns, and system performance with detailed analytics.
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* How it Works Section */}
      <section id="how-it-works" className="py-24 px-6 bg-card border-y border-border">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-4xl font-bold mb-4">How NimbusFS works</h2>
            <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
              A simple yet powerful approach to distributed file storage.
            </p>
          </div>
          
          <div className="grid md:grid-cols-3 gap-8">
            <div className="text-center">
              <div className="w-16 h-16 rounded-full bg-primary/10 border-2 border-primary flex items-center justify-center mx-auto mb-6">
                <span className="text-2xl font-bold text-primary">1</span>
              </div>
              <h3 className="text-xl font-semibold mb-3">Upload your files</h3>
              <p className="text-muted-foreground">
                Simply drag and drop or select files to upload. We handle files of any size efficiently.
              </p>
            </div>
            
            <div className="text-center">
              <div className="w-16 h-16 rounded-full bg-primary/10 border-2 border-primary flex items-center justify-center mx-auto mb-6">
                <span className="text-2xl font-bold text-primary">2</span>
              </div>
              <h3 className="text-xl font-semibold mb-3">Automatic distribution</h3>
              <p className="text-muted-foreground">
                Files are chunked and distributed across multiple nodes for redundancy and fast access.
              </p>
            </div>
            
            <div className="text-center">
              <div className="w-16 h-16 rounded-full bg-primary/10 border-2 border-primary flex items-center justify-center mx-auto mb-6">
                <span className="text-2xl font-bold text-primary">3</span>
              </div>
              <h3 className="text-xl font-semibold mb-3">Access anywhere</h3>
              <p className="text-muted-foreground">
                Retrieve your files instantly from any location. Chunks are assembled seamlessly.
              </p>
            </div>
          </div>

          {/* Visual representation */}
          <div className="mt-16 p-8 rounded-xl border border-border bg-background">
            <div className="flex flex-col lg:flex-row items-center justify-between gap-8">
              <div className="flex flex-col items-center">
                <div className="w-20 h-24 border-2 border-muted-foreground rounded-lg flex items-center justify-center relative">
                  <div className="absolute top-0 right-0 w-5 h-5 bg-background border-l border-b border-muted-foreground rounded-bl-lg" />
                  <span className="text-xs text-muted-foreground">FILE</span>
                </div>
                <p className="text-sm text-muted-foreground mt-2">Your File</p>
              </div>

              <div className="hidden lg:block w-16 h-0.5 bg-primary relative">
                <div className="absolute right-0 top-1/2 -translate-y-1/2 w-0 h-0 border-l-8 border-l-primary border-y-4 border-y-transparent" />
              </div>
              <div className="lg:hidden h-8 w-0.5 bg-primary relative">
                <div className="absolute bottom-0 left-1/2 -translate-x-1/2 w-0 h-0 border-t-8 border-t-primary border-x-4 border-x-transparent" />
              </div>

              <div className="flex flex-col items-center">
                <div className="flex gap-2">
                  <div className="w-8 h-8 bg-chart-1 rounded flex items-center justify-center text-xs font-medium text-primary-foreground">C1</div>
                  <div className="w-8 h-8 bg-chart-2 rounded flex items-center justify-center text-xs font-medium text-primary-foreground">C2</div>
                  <div className="w-8 h-8 bg-chart-3 rounded flex items-center justify-center text-xs font-medium text-primary-foreground">C3</div>
                </div>
                <p className="text-sm text-muted-foreground mt-2">Chunks</p>
              </div>

              <div className="hidden lg:block w-16 h-0.5 bg-primary relative">
                <div className="absolute right-0 top-1/2 -translate-y-1/2 w-0 h-0 border-l-8 border-l-primary border-y-4 border-y-transparent" />
              </div>
              <div className="lg:hidden h-8 w-0.5 bg-primary relative">
                <div className="absolute bottom-0 left-1/2 -translate-x-1/2 w-0 h-0 border-t-8 border-t-primary border-x-4 border-x-transparent" />
              </div>

              <div className="flex flex-col items-center">
                <div className="flex gap-3">
                  <div className="flex flex-col items-center">
                    <div className="w-12 h-12 border-2 border-status-online rounded-lg flex items-center justify-center">
                      <div className="w-2 h-2 bg-status-online rounded-full" />
                    </div>
                    <span className="text-xs text-muted-foreground mt-1">Node 1</span>
                  </div>
                  <div className="flex flex-col items-center">
                    <div className="w-12 h-12 border-2 border-status-online rounded-lg flex items-center justify-center">
                      <div className="w-2 h-2 bg-status-online rounded-full" />
                    </div>
                    <span className="text-xs text-muted-foreground mt-1">Node 2</span>
                  </div>
                  <div className="flex flex-col items-center">
                    <div className="w-12 h-12 border-2 border-status-online rounded-lg flex items-center justify-center">
                      <div className="w-2 h-2 bg-status-online rounded-full" />
                    </div>
                    <span className="text-xs text-muted-foreground mt-1">Node 3</span>
                  </div>
                </div>
                <p className="text-sm text-muted-foreground mt-2">Storage Nodes</p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Pricing Section */}
      <section id="pricing" className="py-24 px-6">
        <div className="max-w-5xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-4xl font-bold mb-4">Simple, transparent pricing</h2>
            <p className="text-lg text-muted-foreground">
              Start free, scale as you grow.
            </p>
          </div>
          
          <div className="grid md:grid-cols-3 gap-6">
            <div className="p-6 rounded-xl border border-border bg-card">
              <h3 className="text-lg font-semibold mb-2">Free</h3>
              <p className="text-muted-foreground text-sm mb-4">For personal projects</p>
              <p className="text-3xl font-bold mb-6">$0<span className="text-lg font-normal text-muted-foreground">/mo</span></p>
              <ul className="space-y-3 mb-6">
                <li className="flex items-center gap-2 text-sm">
                  <div className="w-4 h-4 rounded-full bg-primary/20 flex items-center justify-center">
                    <div className="w-1.5 h-1 border-l border-b border-primary rotate-[-45deg]" />
                  </div>
                  5 GB storage
                </li>
                <li className="flex items-center gap-2 text-sm">
                  <div className="w-4 h-4 rounded-full bg-primary/20 flex items-center justify-center">
                    <div className="w-1.5 h-1 border-l border-b border-primary rotate-[-45deg]" />
                  </div>
                  3 storage nodes
                </li>
                <li className="flex items-center gap-2 text-sm">
                  <div className="w-4 h-4 rounded-full bg-primary/20 flex items-center justify-center">
                    <div className="w-1.5 h-1 border-l border-b border-primary rotate-[-45deg]" />
                  </div>
                  Basic analytics
                </li>
              </ul>
              <Link href="/login" className="block w-full py-2 text-center rounded-lg border border-border hover:bg-secondary transition-colors text-sm font-medium">
                Get started
              </Link>
            </div>

            <div className="p-6 rounded-xl border-2 border-primary bg-card relative">
              <div className="absolute -top-3 left-1/2 -translate-x-1/2 px-3 py-1 bg-primary text-primary-foreground text-xs font-medium rounded-full">
                Popular
              </div>
              <h3 className="text-lg font-semibold mb-2">Pro</h3>
              <p className="text-muted-foreground text-sm mb-4">For growing teams</p>
              <p className="text-3xl font-bold mb-6">$29<span className="text-lg font-normal text-muted-foreground">/mo</span></p>
              <ul className="space-y-3 mb-6">
                <li className="flex items-center gap-2 text-sm">
                  <div className="w-4 h-4 rounded-full bg-primary/20 flex items-center justify-center">
                    <div className="w-1.5 h-1 border-l border-b border-primary rotate-[-45deg]" />
                  </div>
                  100 GB storage
                </li>
                <li className="flex items-center gap-2 text-sm">
                  <div className="w-4 h-4 rounded-full bg-primary/20 flex items-center justify-center">
                    <div className="w-1.5 h-1 border-l border-b border-primary rotate-[-45deg]" />
                  </div>
                  10 storage nodes
                </li>
                <li className="flex items-center gap-2 text-sm">
                  <div className="w-4 h-4 rounded-full bg-primary/20 flex items-center justify-center">
                    <div className="w-1.5 h-1 border-l border-b border-primary rotate-[-45deg]" />
                  </div>
                  Advanced analytics
                </li>
                <li className="flex items-center gap-2 text-sm">
                  <div className="w-4 h-4 rounded-full bg-primary/20 flex items-center justify-center">
                    <div className="w-1.5 h-1 border-l border-b border-primary rotate-[-45deg]" />
                  </div>
                  Priority support
                </li>
              </ul>
              <Link href="/login" className="block w-full py-2 text-center rounded-lg bg-primary text-primary-foreground hover:opacity-90 transition-opacity text-sm font-medium">
                Get started
              </Link>
            </div>

            <div className="p-6 rounded-xl border border-border bg-card">
              <h3 className="text-lg font-semibold mb-2">Enterprise</h3>
              <p className="text-muted-foreground text-sm mb-4">For large organizations</p>
              <p className="text-3xl font-bold mb-6">Custom</p>
              <ul className="space-y-3 mb-6">
                <li className="flex items-center gap-2 text-sm">
                  <div className="w-4 h-4 rounded-full bg-primary/20 flex items-center justify-center">
                    <div className="w-1.5 h-1 border-l border-b border-primary rotate-[-45deg]" />
                  </div>
                  Unlimited storage
                </li>
                <li className="flex items-center gap-2 text-sm">
                  <div className="w-4 h-4 rounded-full bg-primary/20 flex items-center justify-center">
                    <div className="w-1.5 h-1 border-l border-b border-primary rotate-[-45deg]" />
                  </div>
                  Custom node config
                </li>
                <li className="flex items-center gap-2 text-sm">
                  <div className="w-4 h-4 rounded-full bg-primary/20 flex items-center justify-center">
                    <div className="w-1.5 h-1 border-l border-b border-primary rotate-[-45deg]" />
                  </div>
                  SLA guarantee
                </li>
                <li className="flex items-center gap-2 text-sm">
                  <div className="w-4 h-4 rounded-full bg-primary/20 flex items-center justify-center">
                    <div className="w-1.5 h-1 border-l border-b border-primary rotate-[-45deg]" />
                  </div>
                  24/7 support
                </li>
              </ul>
              <Link href="/login" className="block w-full py-2 text-center rounded-lg border border-border hover:bg-secondary transition-colors text-sm font-medium">
                Contact sales
              </Link>
            </div>
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="py-24 px-6 bg-primary/5 border-t border-border">
        <div className="max-w-3xl mx-auto text-center">
          <h2 className="text-3xl md:text-4xl font-bold mb-4">Ready to get started?</h2>
          <p className="text-lg text-muted-foreground mb-8">
            Join thousands of developers who trust NimbusFS for their distributed storage needs.
          </p>
          <Link href="/login" className="inline-flex px-8 py-3 rounded-lg bg-primary text-primary-foreground font-medium hover:opacity-90 transition-opacity">
            Start for free
          </Link>
        </div>
      </section>

      {/* Footer */}
      <footer className="py-12 px-6 border-t border-border">
        <div className="max-w-7xl mx-auto">
          <div className="flex flex-col md:flex-row items-center justify-between gap-6">
            <div className="flex items-center gap-2">
              <div className="w-6 h-6 rounded bg-primary flex items-center justify-center">
                <div className="w-3 h-3 border-2 border-primary-foreground rounded-sm" />
              </div>
              <span className="font-semibold">NimbusFS</span>
            </div>
            
            <div className="flex items-center gap-6 text-sm text-muted-foreground">
              <a href="#" className="hover:text-foreground transition-colors">Privacy</a>
              <a href="#" className="hover:text-foreground transition-colors">Terms</a>
              <a href="#" className="hover:text-foreground transition-colors">Documentation</a>
              <a href="#" className="hover:text-foreground transition-colors">Support</a>
            </div>
            
            <p className="text-sm text-muted-foreground">
              2024 NimbusFS. All rights reserved.
            </p>
          </div>
        </div>
      </footer>
    </div>
  );
}
