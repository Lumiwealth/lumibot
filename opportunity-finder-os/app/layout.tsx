import type { Metadata } from "next";
import Link from "next/link";
import { AppSidebar } from "@/components/AppSidebar";
import "./globals.css";

export const metadata: Metadata = {
  title: "Opportunity Finder OS",
  description: "Local-first pipeline OS for consulting, fractional, startup, and family office opportunities.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>
        <div className="flex min-h-screen">
          <AppSidebar />
          <main className="flex-1">
            <div className="border-b border-slate-800 bg-slate-950/55 px-5 py-4 backdrop-blur lg:hidden">
              <Link href="/" className="text-base font-bold text-white">
                Opportunity Finder OS
              </Link>
            </div>
            <div className="mx-auto max-w-7xl px-5 py-8 lg:px-8">{children}</div>
          </main>
        </div>
      </body>
    </html>
  );
}
