import type React from "react"
import type { Metadata } from "next"
import { Inter } from "next/font/google"
import "./globals.css"
import { ThemeProvider } from "@/components/theme-provider"
import Sidebar from "@/components/sidebar"

const inter = Inter({ 
  subsets: ["latin"],
  weight: ['400', '500', '600', '700'],
  display: 'swap'
})

export const metadata: Metadata = {
  title: "Lucille Admin UI",
  description: "Admin interface for Lucille data pipeline management",
  generator: 'v0.dev',
  openGraph: {
    title: "Lucille Admin UI",
    description: "Admin interface for Lucille data pipeline management",
    type: "website"
  },
  twitter: {
    card: "summary_large_image",
    title: "Lucille Admin UI",
    description: "Admin interface for Lucille data pipeline management"
  }
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className={`${inter.className} bg-body-gradient`}>
        <ThemeProvider attribute="class" defaultTheme="light" enableSystem={false} disableTransitionOnChange forcedTheme="light">
          <div className="flex min-h-screen">
            <Sidebar />
            <main className="flex-1 w-0">{children}</main>
          </div>
        </ThemeProvider>
      </body>
    </html>
  )
}