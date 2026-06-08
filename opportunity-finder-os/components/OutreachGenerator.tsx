import { generateOutreach } from "@/lib/outreach";
import type { LeadRecord } from "@/lib/types";

export function OutreachGenerator({ lead }: { lead: LeadRecord }) {
  const drafts = generateOutreach(lead);

  return (
    <div className="grid gap-4 xl:grid-cols-2">
      {Object.entries(drafts).map(([key, draft]) => (
        <div key={key} className="rounded-2xl border border-slate-800 bg-slate-950/60 p-5">
          <div className="mb-3 flex items-center justify-between gap-3">
            <h3 className="font-semibold text-white">{draft.title}</h3>
            <span className="rounded-full bg-slate-800 px-2 py-0.5 text-xs text-slate-400">Template</span>
          </div>
          <textarea
            readOnly
            value={draft.body}
            className="min-h-72 w-full resize-y rounded-xl border border-slate-800 bg-slate-950 p-4 text-sm leading-6 text-slate-200 outline-none focus:ring-2 focus:ring-teal-400"
          />
        </div>
      ))}
    </div>
  );
}
