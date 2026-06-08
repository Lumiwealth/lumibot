import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";
import type { LeadRecord } from "@/lib/types";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatCurrency(value: number) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

export function formatDate(value?: Date | string | null) {
  if (!value) return "Not set";
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(new Date(value));
}

export function formatDateInput(value?: Date | string | null) {
  if (!value) return "";
  return new Date(value).toISOString().slice(0, 10);
}

export function slugify(value: string) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)+/g, "");
}

export function clampScore(value: number) {
  if (Number.isNaN(value)) return 1;
  return Math.max(1, Math.min(5, Math.round(value)));
}

export function serializeLead(lead: {
  [K in keyof LeadRecord]: LeadRecord[K] | Date;
}): LeadRecord {
  return {
    ...lead,
    last_contacted_date: lead.last_contacted_date ? new Date(lead.last_contacted_date).toISOString() : null,
    follow_up_date: lead.follow_up_date ? new Date(lead.follow_up_date).toISOString() : null,
    created_at: new Date(lead.created_at).toISOString(),
    updated_at: new Date(lead.updated_at).toISOString(),
  } as LeadRecord;
}
