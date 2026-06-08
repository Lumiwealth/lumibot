import Link from "next/link";
import { Download, Plus } from "lucide-react";
import { DashboardStats } from "@/components/DashboardStats";
import { FollowUpList } from "@/components/FollowUpList";
import { LeadTable } from "@/components/LeadTable";
import { PipelineBoard } from "@/components/PipelineBoard";
import { buttonVariants } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { EmptyState } from "@/components/EmptyState";
import { prisma } from "@/lib/prisma";
import { serializeLead, cn } from "@/lib/utils";

export const dynamic = "force-dynamic";

export default async function DashboardPage() {
  const leads = (await prisma.lead.findMany({ orderBy: [{ fit_score: "desc" }, { updated_at: "desc" }] })).map(serializeLead);
  const topLeads = leads.slice(0, 10);
  const weekFromNow = new Date();
  weekFromNow.setDate(weekFromNow.getDate() + 7);
  const followUps = leads
    .filter((lead) => lead.follow_up_date && new Date(lead.follow_up_date) <= weekFromNow && !["Won", "Lost"].includes(lead.stage))
    .sort((a, b) => new Date(a.follow_up_date || 0).getTime() - new Date(b.follow_up_date || 0).getTime())
    .slice(0, 6);

  return (
    <div className="space-y-8">
      <div className="flex flex-col justify-between gap-4 lg:flex-row lg:items-end">
        <div>
          <p className="text-sm font-semibold uppercase tracking-[0.2em] text-teal-200">Founder sales OS</p>
          <h1 className="mt-2 text-4xl font-bold tracking-tight text-white">Who should Rico contact today?</h1>
          <p className="mt-3 max-w-3xl text-slate-400">
            Score and prioritize consulting, fractional, family office, institutional, and startup opportunities tied to
            LCS Engine proof-of-work.
          </p>
        </div>
        <div className="flex flex-wrap gap-3">
          <Link href="/api/export" className={cn(buttonVariants({ variant: "outline" }))}>
            <Download className="h-4 w-4" />
            Export CSV
          </Link>
          <Link href="/leads/new" className={cn(buttonVariants())}>
            <Plus className="h-4 w-4" />
            Add lead
          </Link>
        </div>
      </div>

      {leads.length === 0 ? (
        <EmptyState
          title="No leads yet"
          description="Seed the database or add the first opportunity to start scoring and generating outreach."
        />
      ) : (
        <>
          <DashboardStats leads={leads} />
          <div className="grid gap-6 xl:grid-cols-[1.5fr_1fr]">
            <Card>
              <CardHeader>
                <CardTitle>Top 10 scored leads</CardTitle>
                <CardDescription>Highest-fit opportunities to contact, research, or follow up with.</CardDescription>
              </CardHeader>
              <CardContent>
                <LeadTable leads={topLeads} />
              </CardContent>
            </Card>
            <FollowUpList leads={followUps} title="Next actions due this week" />
          </div>
          <PipelineBoard leads={leads} />
        </>
      )}
    </div>
  );
}
