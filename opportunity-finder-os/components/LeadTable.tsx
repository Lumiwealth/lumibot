import Link from "next/link";
import { ArrowUpRight } from "lucide-react";
import { CategoryBadge } from "@/components/CategoryBadge";
import { PriorityBadge } from "@/components/PriorityBadge";
import { StageBadge } from "@/components/StageBadge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import type { LeadRecord } from "@/lib/types";
import { formatDate } from "@/lib/utils";

export function LeadTable({ leads }: { leads: LeadRecord[] }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Lead</TableHead>
          <TableHead>Category</TableHead>
          <TableHead>Score</TableHead>
          <TableHead>Priority</TableHead>
          <TableHead>Stage</TableHead>
          <TableHead>Next action</TableHead>
          <TableHead>Follow-up</TableHead>
          <TableHead className="text-right">Open</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {leads.map((lead) => (
          <TableRow key={lead.id}>
            <TableCell>
              <div className="font-semibold text-white">{lead.company_name}</div>
              <div className="text-xs text-slate-400">
                {lead.contact_name || "No contact yet"}
                {lead.contact_title ? `, ${lead.contact_title}` : ""}
              </div>
            </TableCell>
            <TableCell>
              <CategoryBadge category={lead.category} />
            </TableCell>
            <TableCell>
              <div className="flex items-center gap-3">
                <div className="h-2 w-20 overflow-hidden rounded-full bg-slate-800">
                  <div className="h-full rounded-full bg-teal-400" style={{ width: `${lead.fit_score}%` }} />
                </div>
                <span className="font-semibold text-white">{lead.fit_score}</span>
              </div>
            </TableCell>
            <TableCell>
              <PriorityBadge priority={lead.priority_level} />
            </TableCell>
            <TableCell>
              <StageBadge stage={lead.stage} />
            </TableCell>
            <TableCell className="max-w-xs truncate">{lead.next_action || "Research decision maker"}</TableCell>
            <TableCell>{formatDate(lead.follow_up_date)}</TableCell>
            <TableCell className="text-right">
              <Link href={`/leads/${lead.id}`} className="inline-flex text-teal-200 hover:text-teal-100">
                <ArrowUpRight className="h-4 w-4" />
              </Link>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
