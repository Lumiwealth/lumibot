import Link from "next/link";
import { CategoryBadge } from "@/components/CategoryBadge";
import { StageBadge } from "@/components/StageBadge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { LeadRecord } from "@/lib/types";
import { formatDate } from "@/lib/utils";

export function LeadDetailPanel({ lead }: { lead: LeadRecord }) {
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center gap-2">
          <CategoryBadge category={lead.category} />
          <StageBadge stage={lead.stage} />
        </div>
        <CardTitle className="text-2xl">{lead.company_name}</CardTitle>
        <CardDescription>{lead.personalized_angle || "Add a sharper personalized angle before outreach."}</CardDescription>
      </CardHeader>
      <CardContent className="grid gap-4 text-sm md:grid-cols-2">
        <Info label="Contact" value={lead.contact_name || "Unknown"} />
        <Info label="Title" value={lead.contact_title || "Unknown"} />
        <Info label="Email" value={lead.contact_email || "Not set"} />
        <Info label="Location" value={lead.location || "Not set"} />
        <Info label="Estimated budget" value={lead.estimated_budget || "Unknown"} />
        <Info label="Revenue potential" value={lead.monthly_revenue_potential || "Not estimated"} />
        <Info label="Suggested offer" value={lead.suggested_offer || "Not generated"} />
        <Info label="Follow-up" value={formatDate(lead.follow_up_date)} />
        <Info label="Warm intro" value={lead.warm_intro_source || "No warm intro yet"} />
        <Info label="Confidence" value={lead.confidence_level || "Medium"} />
        <div className="md:col-span-2">
          <div className="text-xs uppercase tracking-wide text-slate-500">Links</div>
          <div className="mt-1 flex flex-wrap gap-3">
            {lead.website ? (
              <Link className="text-teal-200 hover:text-teal-100" href={lead.website}>
                Website
              </Link>
            ) : null}
            {lead.linkedin_url ? (
              <Link className="text-teal-200 hover:text-teal-100" href={lead.linkedin_url}>
                LinkedIn
              </Link>
            ) : null}
            {!lead.website && !lead.linkedin_url ? <span className="text-slate-400">No links saved</span> : null}
          </div>
        </div>
        <div className="md:col-span-2">
          <div className="text-xs uppercase tracking-wide text-slate-500">Notes</div>
          <p className="mt-1 whitespace-pre-wrap text-slate-300">{lead.notes || "No notes yet."}</p>
        </div>
      </CardContent>
    </Card>
  );
}

function Info({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-xs uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-1 text-slate-200">{value}</div>
    </div>
  );
}
