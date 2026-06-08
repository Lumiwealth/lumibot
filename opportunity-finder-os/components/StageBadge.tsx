import { Badge } from "@/components/ui/badge";

const variants = {
  Won: "green",
  Lost: "red",
  "Proposal Sent": "purple",
  "Call Booked": "default",
  "Followed Up": "amber",
  Sent: "default",
  Nurture: "secondary",
} as const;

export function StageBadge({ stage }: { stage: string }) {
  return <Badge variant={variants[stage as keyof typeof variants] || "outline"}>{stage}</Badge>;
}
