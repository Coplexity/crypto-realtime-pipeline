'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { NAV_ITEMS } from '@/lib/constants';

export default function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="sidebar">
      <div className="sidebar-brand">
        <div className="sidebar-brand-icon">₿</div>
        <div className="sidebar-brand-text">
          <span className="sidebar-brand-name">CryptoStream</span>
          <span className="sidebar-brand-sub">HUST Big Data</span>
        </div>
      </div>

      <nav className="sidebar-nav">
        {NAV_ITEMS.map((item) => {
          const isActive =
            item.href === '/'
              ? pathname === '/'
              : pathname.startsWith(item.href);

          return (
            <Link
              key={item.href}
              href={item.href}
              className={`sidebar-link ${isActive ? 'active' : ''}`}
              id={`nav-${item.label.toLowerCase()}`}
            >
              <span className="sidebar-link-icon">{item.icon}</span>
              <span className="sidebar-link-label">{item.label}</span>
            </Link>
          );
        })}
      </nav>

      <div className="sidebar-footer">
        <div style={{ fontSize: '11px', color: 'var(--text-tertiary)' }}>
          <div style={{ marginBottom: '4px', fontWeight: 600, color: 'var(--text-secondary)' }}>
            Nhóm 01 — HUST
          </div>
          <div>Lưu trữ & Xử lý DLLD</div>
          <div style={{ marginTop: '4px', fontFamily: 'var(--font-mono)', fontSize: '10px' }}>
            v2.0 • Lambda Architecture
          </div>
        </div>
      </div>
    </aside>
  );
}
