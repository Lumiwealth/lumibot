import Link from "next/link";
import { Download, Plus, Search } from "lucide-react";
import { EmptyState } from "@/components/EmptyState";
import { LeadTable } from "@/components/LeadTable";
import { buttonVariants } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { LEAD_CATEGORIES, PIPELINE_STAGES, PRIORITY_LEVELS } from "@/lib/constants";
import { prisma } from "@/lib/prisma";
import type { LeadRecord } from "@/lib/types";
import { cn, serializeLead } from "@/lib/utils";

type SearchParams = {
  q?: string;
  category?: string;
  stage?: string;
  priority?: string;
  sort?: string;
};

export default async function LeadsPage({ searchParams }: { searchParams: Promise<SearchParams> }) {
  const params = await searchParams;
  const leads = (await prisma.lead.findMany({ orderBy: { updated_at: "desc" } })).map(serializeLead);
  const filtered = sortLeads(filterLeads(leads, params), params.sort || "score");

  return (
    <div className="space-y-6">
      <div className="flex flex-col justify-between gap-4 lg:flex-row lg:items-end">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-white">Lead database</h1>
          <p className="mt-2 text-slate-400">Filter, score, and prioritize the opportunities most likely to produce $10K-$20K/month.</p>
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

      <Card>
        <CardHeader>
          <CardTitle>Search and filters</CardTitle>
          <CardDescription>Search company, contact, offer, notes, or personalized angle.</CardDescription>
        </CardHeader>
        <CardContent>
          <form className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
            <div className="relative xl:col-span-2">
              <Search className="absolute left-3 top-3 h-4 w-4 text-slate-500" />
              <Input name="q" defaultValue={params.q || ""} placeholder="Search leads..." className="pl-9" />
            </div>
            <Select name="category" defaultValue={params.category || ""} options={["", ...LEAD_CATEGORIES]} />
            <Select name="stage" defaultValue={params.stage || ""} options={["", ...PIPELINE_STAGES]} />
            <Select name="priority" defaultValue={params.priority || ""} options={["", ...PRIORITY_LEVELS]} />
            <Select name="sort" defaultValue={params.sort || "score"} options={["score", "updated", "company", "follow_up"]} />
            <button className={cn(buttonVariants(), "xl:col-start-5")} type="submit">
              Apply filters
            </button>
          </form>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>{filtered.length} leads</CardTitle>
          <CardDescription>Click any lead to view score breakdown, outreach drafts, and stage updates.</CardDescription>
        </CardHeader>
        <CardContent>
          {filtered.length === 0 ? (
            <EmptyState title="No matching leads" description="Try clearing filters or add a new lead to the pipeline." />
          ) : (
            <LeadTable leads={filtered} />
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function Select({
  name,
  defaultValue,
  options,
}: {
  name: string;
  defaultValue: string;
  options: readonly string[];
}) {
  return (
    <select
      name={name}
      defaultValue={defaultValue}
      className="h-10 rounded-lg border border-slate-700 bg-slate-950/70 px-3 text-sm text-slate-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-teal-400"
    >
      {options.map((option) => (
        <option key={option || "all"} value={option}>
          {option || `All ${name}`}
        </option>
      ))}
    </select>
  );
}

function filterLeads(leads: LeadRecord[], params: SearchParams) {
  const query = params.q?.toLowerCase().trim();

  return leads.filter((lead) => {
    const matchesQuery = query
      ? [
          lead.company_name,
          lead.category,
          lead.contact_name,
          lead.contact_title,
          lead.suggested_offer,
          lead.personalized_angle,
          lead.notes,
        ]
          .filter(Boolean)
          .join(" ")
          .toLowerCase()
          .includes(query)
      : true;

    return (
      matchesQuery &&
      (!params.category || lead.category === params.category) &&
      (!params.stage || lead.stage === params.stage) &&
      (!params.priority || lead.priority_level === params.priority)
    );
  });
}

function sortLeads(leads: LeadRecord[], sort: string) {
  return [...leads].sort((a, b) => {
    if (sort === "company") return a.company_name.localeCompare(b.company_name);
    if (sort === "updated") return new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime();
    if (sort === "follow_up") {
      return new Date(a.follow_up_date || "2999-01-01").getTime() - new Date(b.follow_up_date || "2999-01-01").getTime();
    }
    return b.fit_score - a.fit_score;
  });
}
