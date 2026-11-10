import * as React from "react"
import { cn } from "@/lib/utils"
import styles from "./card.module.css"

function Card({ className, ...props }: React.ComponentProps<"div">) {
  return <div className={cn(styles.card, className)} {...props} />
}

function CardHeader({ className, ...props }: React.ComponentProps<"div">) {
  return <div className={cn(styles.header, className)} {...props} />
}

function CardTitle({ className, ...props }: React.ComponentProps<"div">) {
  return <div className={cn(styles.title, className)} {...props} />
}

function CardDescription({ className, ...props }: React.ComponentProps<"div">) {
  return <div className={cn(styles.description, className)} {...props} />
}

function CardAction({ className, ...props }: React.ComponentProps<"div">) {
  return <div className={cn(styles.action, className)} {...props} />
}

function CardContent({ className, ...props }: React.ComponentProps<"div">) {
  return <div className={cn(styles.content, className)} {...props} />
}

function CardFooter({ className, ...props }: React.ComponentProps<"div">) {
  return <div className={cn(styles.footer, className)} {...props} />
}

export {
  Card,
  CardHeader,
  CardFooter,
  CardTitle,
  CardAction,
  CardDescription,
  CardContent,
}
