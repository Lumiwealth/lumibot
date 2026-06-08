"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  BarChart3,
  CalendarClock,
  Database,
  Home,
  Mail,
  Settings,
  Sparkles,
  TrendingUp,
} from "lucide-react";
import { cn } from "@/lib/utils";

const navItems = [
  { href: "/", label: "Dashboard", icon: Home },
  { href: "/leads", label: "Leads", icon: Database },
  { href: "/leads/new", label: "Add Lead", icon: Sparkles },
  { href: "/outreach", label: "Outreach", icon: Mail },
  { href: "/forecast", label: "Forecast", icon: TrendingUp },
  { href: "/followups", label: "Follow-ups", icon: CalendarClock },
  { href: "/settings", label: "Settings", icon: Settings },
];

export function AppSidebar() {
  const pathname = usePathname();

  return (
    <aside className="hidden min-h-screen w-72 shrink-0 border-r border-slate-800 bg-slate-950/70 p-5 lg:block">
      <Link href="/" className="mb-8 flex items-center gap-3">
        <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-teal-400 text-slate-950 shadow-lg shadow-teal-400/20">
          <BarChart3 className="h-6 w-6" />
        </div>
        <div>
          <div className="text-lg font-bold text-white">Opportunity Finder OS</div>
          <div className="text-xs text-slate-400">Rico&apos;s $10K-$20K pipeline</div>
        </div>
      </Link>

      <nav className="space-y-1">
        {navItems.map((item) => {
          const active = item.href === "/" ? pathname === "/" : pathname.startsWith(item.href);
          const Icon = item.icon;

          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex items-center gap-3 rounded-xl px-3 py-2.5 text-sm font-medium transition",
                active
                  ? "bg-teal-400/15 text-teal-100 ring-1 ring-teal-400/20"
                  : "text-slate-400 hover:bg-slate-900 hover:text-white",
              )}
            >
              <Icon className="h-4 w-4" />
              {item.label}
            </Link>
          );
        })}
      </nav>

      <div className="mt-8 rounded-2xl border border-slate-800 bg-slate-900/70 p-4">
        <p className="text-xs font-semibold uppercase tracking-wide text-teal-200">Daily question</p>
        <p className="mt-2 text-sm leading-6 text-slate-300">
          Who should Rico contact today, why are they a fit, what should he pitch, and what should he say?
        </p>
      </div>
    </aside>
  );
}
