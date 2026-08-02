import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import Sidebar from "@/components/organisms/layout/Sidebar";
import { Providers } from "@/components/providers/Providers";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "CUIN v2 | Identity Intelligence Platform",
  description: "Production-grade identity resolution for banking with explainable AI",
};

// Runs before hydration so <html> gets the right theme class on first
// paint -- without this, the page briefly renders with no theme class
// (light-mode CSS defaults) until Providers' client-side effect catches
// up, which can look like a stuck-light-mode flash on a slow paint or
// after a hot-reload. Reads the same zustand-persist key/shape the store
// itself writes (see stores/useAppStore.ts's `persist({name: 'cuin-storage'})`).
const THEME_INIT_SCRIPT = `
(function() {
  try {
    var raw = localStorage.getItem('cuin-storage');
    var theme = 'dark';
    if (raw) {
      var parsed = JSON.parse(raw);
      if (parsed && parsed.state && (parsed.state.theme === 'light' || parsed.state.theme === 'dark')) {
        theme = parsed.state.theme;
      }
    }
    document.documentElement.classList.add(theme);
  } catch (e) {
    document.documentElement.classList.add('dark');
  }
})();
`;

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <script dangerouslySetInnerHTML={{ __html: THEME_INIT_SCRIPT }} />
      </head>
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased bg-gray-50 dark:bg-gray-950 text-gray-900 dark:text-gray-100 transition-colors duration-300`}
      >
        <Providers>
          <div className="flex h-screen overflow-hidden">
            {/* Sidebar Navigation */}
            <Sidebar />

            {/* Main Content */}
            <main className="flex-1 overflow-y-auto">
              <div className="container mx-auto px-6 py-8">
                {children}
              </div>
            </main>
          </div>
        </Providers>
      </body>
    </html>
  );
}
