import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import App from './App';
import { vi, expect } from 'vitest';

global.WebSocket = class {
  constructor(url) {
    this.url = url;
    setTimeout(() => {
      this.onmessage &&
        this.onmessage({
          data: JSON.stringify({
            status: 'completed',
            uploaded: 1,
            total: 1,
            rows: 1,
          }),
        });
    }, 0);
  }
  close() {}
};

vi.mock('axios', () => ({
  default: {
    post: vi.fn(() => Promise.resolve({ data: { job_id: 'job-1' } })),
  },
}));

test('uploads file and displays status', async () => {
  render(<App />);
  const apiInput = screen.getByPlaceholderText('API Key');
  fireEvent.change(apiInput, { target: { value: 'testkey' } });

  const file = new File(['hello'], 'hello.csv', { type: 'text/csv' });
  const fileInput = screen.getByTestId('file-input');
  fireEvent.change(fileInput, { target: { files: [file] } });

  fireEvent.click(screen.getByText('Upload'));

  await waitFor(() => screen.getByText(/Status: completed/i));
  expect(screen.getByText(/Rows: 1/)).toBeInTheDocument();
});
