import * as React from "react";

import { cn } from "@/lib/utils";

type ButtonProps = React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "default" | "secondary" | "outline" | "ghost";
};

const variantClasses: Record<NonNullable<ButtonProps["variant"]>, string> = {
  default: "tap-button--primary",
  secondary: "tap-button--secondary",
  outline: "",
  ghost: "",
};

export function Button({ className, variant = "default", type = "button", ...props }: ButtonProps) {
  return (
    <button
      type={type}
      className={cn(
        "tap-button",
        variantClasses[variant],
        className,
      )}
      {...props}
    />
  );
}
