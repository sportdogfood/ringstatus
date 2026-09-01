import "./globals.css";

export const metadata = {
  title: "Lainey Posa — Equestrian",
  description: "A working equestrian record built from the Lainey page stack."
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
