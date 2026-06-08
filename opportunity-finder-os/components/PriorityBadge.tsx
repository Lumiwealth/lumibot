import { Badge } from "@/components/ui/badge";

const variants = {
  "Must Contact": "green",
  "Strong Lead": "default",
  "Worth Testing": "purple",
  "Low Priority": "amber",
  "Ignore for Now": "red",
} as const;

export function PriorityBadge({ priority }: { priority: string }) {
  return <Badge variant={variants[priority as keyof typeof variants] || "secondary"}>{priority}</Badge>;
}
