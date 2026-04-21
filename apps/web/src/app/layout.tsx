import type { Metadata } from "next";

import { EntityDrawer, EntityDrawerProvider } from "@/components/entity";

import "./globals.css";

export const metadata: Metadata = {
  title: "GitHub Insight",
  description: "Streaming + batch analytics for the GitHub open-source ecosystem.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body suppressHydrationWarning>
        <EntityDrawerProvider>
          {children}
          <EntityDrawer />
        </EntityDrawerProvider>
      </body>
    </html>
  );
}
