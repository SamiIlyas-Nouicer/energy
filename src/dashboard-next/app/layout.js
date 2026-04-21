import './globals.css';
import Sidebar from '@/components/Sidebar';

export const metadata = {
  title: 'French Energy Intelligence Platform',
  description: 'Real-time monitoring, regional analysis, ML-powered forecasting, and pipeline observability for the French power grid.',
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>
        <div className="layout">
          <Sidebar />
          <main className="main-content">{children}</main>
        </div>
      </body>
    </html>
  );
}
