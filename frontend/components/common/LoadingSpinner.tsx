export default function LoadingSpinner({ message = 'Đang tải dữ liệu...' }: { message?: string }) {
  return (
    <div className="loading-container">
      <div className="spinner" />
      <span>{message}</span>
    </div>
  );
}
