import * as DialogPrimitive from "@radix-ui/react-dialog";
import { cva, type VariantProps } from "class-variance-authority";
import { clsx, type ClassValue } from "clsx";
import { AlertTriangle, Construction, Inbox, Loader2, X } from "lucide-react";
import type { ButtonHTMLAttributes, ReactNode } from "react";
import { twMerge } from "tailwind-merge";

// shadcn/ui-style primitives (copied-in components on Radix + cva + tailwind-merge).
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function Card({ title, action, className, children }: {
  title?: ReactNode; action?: ReactNode; className?: string; children: ReactNode;
}) {
  return (
    <section className={cn("rounded-lg border border-rule bg-panel p-4 shadow-sm", className)}>
      {(title || action) && (
        <header className="mb-3 flex items-center justify-between gap-2">
          <h2 className="text-sm font-semibold text-ink">{title}</h2>
          {action}
        </header>
      )}
      {children}
    </section>
  );
}

const buttonVariants = cva(
  "inline-flex items-center justify-center gap-1.5 rounded-md text-sm font-medium transition-colors " +
    "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent disabled:opacity-50",
  {
    variants: {
      variant: {
        default: "bg-accent text-white hover:opacity-90",
        outline: "border border-rule bg-panel text-ink hover:bg-surface",
        danger: "bg-bad text-white hover:opacity-90",
        ghost: "text-ink hover:bg-surface",
      },
      size: { sm: "h-8 px-3", md: "h-9 px-4", lg: "h-11 px-6 text-base" },
    },
    defaultVariants: { variant: "default", size: "md" },
  },
);

export function Button({ className, variant, size, ...props }:
  ButtonHTMLAttributes<HTMLButtonElement> & VariantProps<typeof buttonVariants>) {
  return <button className={cn(buttonVariants({ variant, size }), className)} {...props} />;
}

export function Badge({ color, children }: { color?: string; children: ReactNode }) {
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border border-rule px-2 py-0.5 text-xs text-ink">
      {color && <span className="h-2 w-2 rounded-full" style={{ background: color }} />}
      {children}
    </span>
  );
}

export function Dialog({ open, onOpenChange, title, children, side = false }: {
  open: boolean; onOpenChange: (open: boolean) => void; title: string; children: ReactNode; side?: boolean;
}) {
  return (
    <DialogPrimitive.Root open={open} onOpenChange={onOpenChange}>
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="fixed inset-0 z-40 bg-black/40" />
        <DialogPrimitive.Content
          className={cn(
            "fixed z-50 overflow-y-auto border border-rule bg-panel p-5 text-ink shadow-xl",
            side
              ? "right-0 top-0 h-full w-full max-w-2xl"
              : "left-1/2 top-1/2 w-full max-w-md -translate-x-1/2 -translate-y-1/2 rounded-lg",
          )}
        >
          <div className="mb-4 flex items-center justify-between">
            <DialogPrimitive.Title className="text-base font-semibold">{title}</DialogPrimitive.Title>
            <DialogPrimitive.Close aria-label="Close" className="rounded p-1 hover:bg-surface">
              <X size={18} />
            </DialogPrimitive.Close>
          </div>
          <DialogPrimitive.Description className="sr-only">{title}</DialogPrimitive.Description>
          {children}
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  );
}

/** Empty, error, loading and not-built states: every panel has one (spec §15.9). */
export function PanelState({ kind, message }: {
  kind: "empty" | "error" | "loading" | "notbuilt"; message?: string;
}) {
  const Icon = { empty: Inbox, error: AlertTriangle, loading: Loader2, notbuilt: Construction }[kind];
  const text = message ?? {
    empty: "No data yet",
    error: "Could not load",
    loading: "Loading…",
    notbuilt: "Not built yet",
  }[kind];
  return (
    <div role={kind === "error" ? "alert" : "status"}
      className="flex min-h-[120px] flex-col items-center justify-center gap-2 text-center text-sm text-muted">
      <Icon size={20} className={kind === "loading" ? "animate-spin" : ""}
        style={kind === "error" ? { color: "var(--bad)" } : undefined} />
      <span>{text}</span>
    </div>
  );
}

/** A query's loading/error/empty state, or its content. */
export function Guard<T>({ query, empty, children }: {
  query: { data?: T; error: unknown; isLoading: boolean };
  empty?: (data: T) => boolean;
  children: (data: T) => ReactNode;
}) {
  if (query.isLoading) return <PanelState kind="loading" />;
  if (query.error) return <PanelState kind="error" message={String((query.error as Error).message ?? query.error)} />;
  if (query.data === undefined || (empty && empty(query.data))) return <PanelState kind="empty" />;
  return <>{children(query.data)}</>;
}
