"use client";

import type { ReactNode } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { buildRevenueForecast } from "@/lib/revenue";
import type { LeadRecord } from "@/lib/types";
import { formatCurrency } from "@/lib/utils";

const COLORS = ["#2dd4bf", "#a78bfa", "#fbbf24", "#60a5fa", "#fb7185", "#34d399", "#f472b6"];

function groupCount(leads: LeadRecord[], key: keyof LeadRecord) {
  const map = new Map<string, number>();
  leads.forEach((lead) => map.set(String(lead[key]), (map.get(String(lead[key])) || 0) + 1));
  return Array.from(map.entries()).map(([name, value]) => ({ name, value }));
}

export function DashboardStats({ leads }: { leads: LeadRecord[] }) {
  const forecast = buildRevenueForecast(leads);
  const byCategory = groupCount(leads, "category");
  const byStage = groupCount(leads, "stage");
  const byPriority = groupCount(leads, "priority_level");
  const revenueByCategory = forecast.byCategory;
  const callsBooked = leads.filter((lead) => lead.stage === "Call Booked").length;
  const proposalsSent = leads.filter((lead) => lead.stage === "Proposal Sent").length;
  const won = leads.filter((lead) => lead.stage === "Won").length;
  const premiumTargets = leads.filter((lead) => lead.fit_score >= 70 && lead.monthly_revenue_potential?.includes("$10K"));

  return (
    <div className="space-y-6">
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <Metric title="Total leads" value={leads.length.toString()} detail="Local SQLite pipeline" />
        <Metric title="Realistic pipeline" value={formatCurrency(forecast.realisticMonthly)} detail="Probability weighted" />
        <Metric title="Best-case pipeline" value={formatCurrency(forecast.bestCaseMonthly)} detail="Open opportunity value" />
        <Metric title="$10K-$20K candidates" value={premiumTargets.length.toString()} detail="High-fit premium retainers" />
        <Metric title="Calls booked" value={callsBooked.toString()} detail="Discovery conversations" />
        <Metric title="Proposals sent" value={proposalsSent.toString()} detail="Closeable opportunities" />
        <Metric title="Won" value={won.toString()} detail="Converted opportunities" />
        <Metric title="Active conversations" value={forecast.activeConversations.toString()} detail="Sent through proposal" />
      </div>

      <div className="grid gap-6 xl:grid-cols-2">
        <ChartCard title="Leads by category" description="Where the opportunity surface is strongest.">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={byCategory}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis dataKey="name" stroke="#94a3b8" tick={{ fontSize: 11 }} interval={0} angle={-20} textAnchor="end" height={80} />
              <YAxis stroke="#94a3b8" allowDecimals={false} />
              <Tooltip contentStyle={{ background: "#0f172a", border: "1px solid #334155", borderRadius: 12 }} />
              <Bar dataKey="value" radius={[8, 8, 0, 0]}>
                {byCategory.map((_, index) => (
                  <Cell key={index} fill={COLORS[index % COLORS.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Pipeline by stage" description="Manual pipeline progress from found to won.">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={byStage}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis dataKey="name" stroke="#94a3b8" tick={{ fontSize: 11 }} interval={0} angle={-20} textAnchor="end" height={80} />
              <YAxis stroke="#94a3b8" allowDecimals={false} />
              <Tooltip contentStyle={{ background: "#0f172a", border: "1px solid #334155", borderRadius: 12 }} />
              <Bar dataKey="value" fill="#2dd4bf" radius={[8, 8, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Revenue potential by category" description="Best-case value using static local assumptions.">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={revenueByCategory}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis dataKey="name" stroke="#94a3b8" tick={{ fontSize: 11 }} interval={0} angle={-20} textAnchor="end" height={80} />
              <YAxis stroke="#94a3b8" tickFormatter={(value) => `$${Number(value) / 1000}K`} />
              <Tooltip
                formatter={(value) => formatCurrency(Number(value))}
                contentStyle={{ background: "#0f172a", border: "1px solid #334155", borderRadius: 12 }}
              />
              <Bar dataKey="value" fill="#a78bfa" radius={[8, 8, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Priority distribution" description="Who should be contacted first.">
          <ResponsiveContainer width="100%" height={280}>
            <PieChart>
              <Pie data={byPriority} dataKey="value" nameKey="name" innerRadius={68} outerRadius={104} paddingAngle={4}>
                {byPriority.map((_, index) => (
                  <Cell key={index} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip contentStyle={{ background: "#0f172a", border: "1px solid #334155", borderRadius: 12 }} />
            </PieChart>
          </ResponsiveContainer>
          <div className="mt-4 grid grid-cols-2 gap-2 text-xs text-slate-300">
            {byPriority.map((item, index) => (
              <div key={item.name} className="flex items-center gap-2">
                <span className="h-2.5 w-2.5 rounded-full" style={{ background: COLORS[index % COLORS.length] }} />
                {item.name}: {item.value}
              </div>
            ))}
          </div>
        </ChartCard>
      </div>
    </div>
  );
}

function Metric({ title, value, detail }: { title: string; value: string; detail: string }) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardDescription>{title}</CardDescription>
        <CardTitle className="text-3xl">{value}</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-xs text-slate-500">{detail}</p>
      </CardContent>
    </Card>
  );
}

function ChartCard({
  title,
  description,
  children,
}: {
  title: string;
  description: string;
  children: ReactNode;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent>{children}</CardContent>
    </Card>
  );
}
