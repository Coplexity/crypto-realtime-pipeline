'use client';

import type { SparkJob } from '@/types';

interface SparkJobsProps {
  jobs: SparkJob[];
}

const STATUS_BADGES: Record<string, string> = {
  running: 'healthy',
  completed: 'degraded',
  failed: 'down',
};

export default function SparkJobs({ jobs }: SparkJobsProps) {
  return (
    <div id="spark-jobs">
      <div style={{ overflowX: 'auto' }}>
        <table className="data-table">
          <thead>
            <tr>
              <th>Job</th>
              <th>Status</th>
              <th>Duration</th>
              <th>Records</th>
            </tr>
          </thead>
          <tbody>
            {jobs.length === 0 ? (
              <tr>
                <td colSpan={4} style={{ textAlign: 'center', padding: '24px', color: 'var(--text-tertiary)' }}>
                  Chưa có Spark jobs nào...
                </td>
              </tr>
            ) : (
              jobs.map((job) => (
                <tr key={job.id}>
                  <td>
                    <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '13px' }}>
                      {job.name}
                    </div>
                    <div style={{ fontSize: '11px', fontFamily: 'var(--font-mono)', color: 'var(--text-tertiary)' }}>
                      {job.id}
                    </div>
                  </td>
                  <td>
                    <span className={`status-badge ${STATUS_BADGES[job.status] || 'degraded'}`}>
                      {job.status === 'running' && '● '}
                      {job.status.toUpperCase()}
                    </span>
                  </td>
                  <td style={{ fontFamily: 'var(--font-mono)', fontSize: '12px' }}>
                    {job.duration}
                  </td>
                  <td style={{ fontFamily: 'var(--font-mono)', fontSize: '12px' }}>
                    {job.records_processed.toLocaleString()}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
