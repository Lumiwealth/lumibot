import { Badge } from "@/components/ui/badge";

export function CategoryBadge({ category }: { category: string }) {
  return <Badge variant="purple">{category}</Badge>;
}
