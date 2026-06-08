import Link from "next/link";
import { OutreachGenerator } from "@/components/OutreachGenerator";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { prisma } from "@/lib/prisma";
import { serializeLead } from "@/lib/utils";

export default async function OutreachPage({ searchParams }: { searchParams: Promise<{ leadId?: string }> }) {
  const params = await searchParams;
  const leads = (await prisma.lead.findMany({ orderBy: [{ fit_score: "desc" }, { company_name: "asc" }] })).map(serializeLead);
  const selected = leads.find((lead) => lead.id === params.leadId) || leads[0];

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight text-white">Outreach generator</h1>
        <p className="mt-2 max-w-3xl text-slate-400">
          Generate cold email, warm intro, LinkedIn DM, follow-ups, proposal copy, agenda, referral ask, and
          reactivation notes from deterministic templates.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Select a lead</CardTitle>
          <CardDescription>No AI APIs are used. Drafts are generated from saved lead fields.</CardDescription>
        </CardHeader>
        <CardContent>
          {leads.length === 0 ? (
            <Link href="/leads/new" className="text-teal-200 hover:text-teal-100">
              Add your first lead
            </Link>
          ) : (
            <form className="flex flex-col gap-3 md:flex-row">
              <select
                name="leadId"
                defaultValue={selected?.id}
                className="h-10 flex-1 rounded-lg border border-slate-700 bg-slate-950/70 px-3 text-sm text-slate-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-teal-400"
              >
                {leads.map((lead) => (
                  <option key={lead.id} value={lead.id}>
                    {lead.company_name} · {lead.fit_score} · {lead.suggested_offer}
                  </option>
                ))}
              </select>
              <button className="h-10 rounded-lg bg-teal-400 px-4 text-sm font-semibold text-slate-950 hover:bg-teal-300">
                Generate drafts
              </button>
            </form>
          )}
        </CardContent>
      </Card>

      {selected ? <OutreachGenerator lead={selected} /> : null}
    </div>
  );
}
