import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const badgeVariants = cva(
  "inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold transition-colors",
  {
    variants: {
      variant: {
        default: "border-transparent bg-teal-400/15 text-teal-200",
        secondary: "border-slate-700 bg-slate-800 text-slate-200",
        outline: "border-slate-700 text-slate-300",
        purple: "border-purple-400/20 bg-purple-400/15 text-purple-200",
        amber: "border-amber-400/20 bg-amber-400/15 text-amber-200",
        red: "border-red-400/20 bg-red-400/15 text-red-200",
        green: "border-emerald-400/20 bg-emerald-400/15 text-emerald-200",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  },
);

export interface BadgeProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof badgeVariants> {}

export function Badge({ className, variant, ...props }: BadgeProps) {
  return <div className={cn(badgeVariants({ variant }), className)} {...props} />;
}
